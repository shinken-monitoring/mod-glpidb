#!/usr/bin/python

# -*- coding: utf-8 -*-

# Copyright (C) 2009-2012:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.


# This Class is a plugin for the Shinken Broker. It is in charge
# to brok information into the glpi database. for the moment
# only Mysql is supported. This code is __imported__ from Broker.
# The managed_brok function is called by Broker for manage the broks. It calls
# the manage_*_brok functions that create queries, and then run queries.

import copy
import time
import datetime
import sys


from shinken.basemodule import BaseModule
from shinken.log import logger

properties = {
    'daemons': ['broker'],
    'type': 'glpidb',
    'external': True,
}


# Called by the plugin manager to get a broker
def get_instance(plugin):
    logger.info("[glpidb] Get a glpidb data module for plugin %s" % plugin.get_name())
    instance = Glpidb_broker(plugin)
    return instance


# Class for the Glpidb Broker
# Get broks and puts them in GLPI database
class Glpidb_broker(BaseModule):
    def __init__(self, modconf):
        BaseModule.__init__(self, modconf)
        
        self.hosts_cache = {}
        self.services_cache = {}

        # Database configuration
        self.host = getattr(modconf, 'host', '127.0.0.1')
        self.user = getattr(modconf, 'user', 'shinken')
        self.password = getattr(modconf, 'password', 'shinken')
        self.database = getattr(modconf, 'database', 'glpidb')
        self.character_set = getattr(modconf, 'character_set', 'utf8')
        logger.info("[glpidb] using '%s' database on %s (user = %s)", self.database, self.host, self.user)

        # Database tables update configuration
        self.update_shinken_state = getattr(modconf, 'update_shinken_state', False)
        self.update_services_events = getattr(modconf, 'update_services_events', False)
        self.update_hosts = getattr(modconf, 'update_hosts', False)
        self.update_services = getattr(modconf, 'update_services', False)
        self.update_acknowledges = getattr(modconf, 'update_acknowledges', False)
        logger.info("[glpidb] updating Shinken state: %s", self.update_shinken_state)
        logger.info("[glpidb] updating services events: %s", self.update_services_events)
        logger.info("[glpidb] updating hosts states: %s", self.update_hosts)
        logger.info("[glpidb] updating services states: %s", self.update_services)
        logger.info("[glpidb] updating acknowledges states: %s", self.update_acknowledges)
        
    def init(self):
        from shinken.db_mysql import DBMysql
        logger.info("[glpidb] Creating a mysql backend : %s (%s)" % (self.host, self.database))
        self.db_backend = DBMysql(self.host, self.user, self.password, self.database, self.character_set)

        logger.info("[glpidb] Connecting to database ...")
        self.db_backend.connect_database()
        logger.info("[glpidb] Connected")

    # Get a brok, parse it, and put in in database
    def manage_brok(self, b):
        # Build initial host state cache
        if b.type == 'initial_host_status':
            host_name = b.data['host_name']
            logger.debug("[glpidb] initial host status : %s", host_name)

            try:
                logger.debug("[glpidb] initial host status : %s : %s", host_name, b.data['customs'])
                self.hosts_cache[host_name] = {'hostsid': b.data['customs']['_HOSTID'], 'itemtype': b.data['customs']['_ITEMTYPE'], 'items_id': b.data['customs']['_ITEMSID'] }
            except:
                self.hosts_cache[host_name] = {'items_id': None}
                logger.debug("[glpidb] no custom _HOSTID and/or _ITEMTYPE and/or _ITEMSID for %s", host_name)
                
            logger.debug("[glpidb] initial host status : %s is %s", host_name, self.hosts_cache[host_name]['items_id'])
        
        # Build initial service state cache
        if b.type == 'initial_service_status':
            host_name = b.data['host_name']
            service_description = b.data['service_description']
            service_id = host_name+"/"+service_description
            logger.debug("[glpidb] initial service status : %s", service_id)
            
            if not host_name in self.hosts_cache or self.hosts_cache[host_name]['items_id'] is None:
                logger.debug("[glpidb] initial service status, host is not defined in Glpi : %s.", host_name)
                return

            try:
                logger.debug("[glpidb] initial service status : %s : %s", service_id, b.data['customs'])
                self.services_cache[service_id] = {'itemtype': b.data['customs']['_ITEMTYPE'], 'items_id': b.data['customs']['_ITEMSID'] }
            except:
                self.services_cache[service_id] = {'items_id': None}
                logger.debug("[glpidb] no custom _ITEMTYPE and/or _ITEMSID for %s", service_id)
                
            logger.debug("[glpidb] initial service status : %s is %s", service_id, self.services_cache[service_id]['items_id'])
        
        # Manage host check result if host is defined in Glpi DB
        if b.type == 'host_check_result':
            host_name = b.data['host_name']
            logger.debug("[glpidb] host check result: %s: %s", host_name, b.data)
            
            # Update Shinken state table 
            if self.update_shinken_state:
                self.record_shinken_state(host_name, '', b)
                
            if host_name in self.hosts_cache and self.hosts_cache[host_name]['items_id'] is not None:
                start = time.time()
                self.record_host_check_result(b)
                logger.debug("[glpidb] host check result: %s, %d seconds", host_name, time.time() - start)
                
        # Manage service check result if service is defined in Glpi DB
        if b.type == 'service_check_result':
            host_name = b.data['host_name']
            service_description = b.data['service_description']
            service_id = host_name+"/"+service_description
            logger.debug("[glpidb] service check result: %s", service_id)
            
            # Update Shinken state table 
            if self.update_shinken_state:
                self.record_shinken_state(host_name, service_description, b)
                
            if host_name in self.hosts_cache and self.hosts_cache[host_name]['items_id'] is not None:
                if service_id in self.services_cache and self.services_cache[service_id]['items_id'] is not None:
                    start = time.time()
                    self.record_service_check_result(b)
                    logger.debug("[glpidb] service check result: %s, %d seconds", service_id, time.time() - start)
                
        return

    ## Host result
    def record_host_check_result(self, b):
        host_name = b.data['host_name']
        host_cache = self.hosts_cache[host_name]
        logger.debug("[glpidb] host check result: %s: %s", host_name, b.data)
        
        if self.update_hosts:
            data = {}
            data['event'] = ("%s \n %s", b.data['output'], b.data['long_output']) if (len(b.data['long_output']) > 0) else b.data['output']
            data['state'] = b.data['state']
            data['state_type'] = b.data['state_type']
            data['last_check'] = datetime.datetime.fromtimestamp( int(b.data['last_chk']) ).strftime('%Y-%m-%d %H:%M:%S')
            data['perf_data'] = b.data['perf_data']
            data['latency'] = b.data['latency']
            data['execution_time'] = b.data['execution_time']
            data['is_acknowledged'] = '1' if b.data['problem_has_been_acknowledged'] else '0'
            
            where_clause = {'items_id': host_cache['items_id'], 'itemtype': host_cache['itemtype']}
            query = self.db_backend.create_update_query('glpi_plugin_monitoring_hosts', data, where_clause)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)

        # Update acknowledge table if host becomes UP
        if self.update_acknowledges and b.data['state_id'] == 0 and b.data['last_state_id'] != 0:
            data = {}
            data['end_time'] = datetime.datetime.fromtimestamp( int(b.data['last_chk']) ).strftime('%Y-%m-%d %H:%M:%S')
            data['expired'] = '1' 

            logger.info("[glpidb] host check data: %s", data)
            where_clause = {'items_id': host_cache['items_id'], 'itemtype': host_cache['itemtype']}
            query = self.db_backend.create_update_query('glpi_plugin_monitoring_acknowledges', data, where_clause)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)

    ## Service result
    def record_service_check_result(self, b):
        host_name = b.data['host_name']
        service_description = b.data['service_description']
        service_id = host_name+"/"+service_description
        service_cache = self.services_cache[service_id]
        logger.debug("[glpidb] service check result: %s: %s", service_id, b.data)
        
        # Insert into serviceevents log table
        if self.update_services_events:
            data = {}
            data['plugin_monitoring_services_id'] = service_cache['items_id']
            data['date'] = datetime.datetime.fromtimestamp( int(b.data['last_chk']) ).strftime('%Y-%m-%d %H:%M:%S')
            data['event'] = ("%s \n %s", b.data['output'], b.data['long_output']) if (len(b.data['long_output']) > 0) else b.data['output']
            data['state'] = b.data['state']
            data['state_type'] = b.data['state_type']
            data['perf_data'] = b.data['perf_data']
            data['latency'] = b.data['latency']
            data['execution_time'] = b.data['execution_time']
            
            query = self.db_backend.create_insert_query('glpi_plugin_monitoring_serviceevents', data)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)

        # Update service state table
        if self.update_services:
            data = {}
            data['event'] = ("%s \n %s", b.data['output'], b.data['long_output']) if (len(b.data['long_output']) > 0) else b.data['output']
            data['state'] = b.data['state']
            data['state_type'] = b.data['state_type']
            data['last_check'] = datetime.datetime.fromtimestamp( int(b.data['last_chk']) ).strftime('%Y-%m-%d %H:%M:%S')
            data['is_acknowledged'] = '1' if b.data['problem_has_been_acknowledged'] else '0'
            
            where_clause = {'id': service_cache['items_id']}
            table = 'glpi_plugin_monitoring_services'
            if service_cache['itemtype'] == 'ServiceCatalog':
                table = 'glpi_plugin_monitoring_servicescatalogs'
            query = self.db_backend.create_update_query(table, data, where_clause)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)

        # Update acknowledge table if service becomes UP
        if self.update_acknowledges and b.data['state_id'] == 0 and b.data['last_state_id'] != 0:
            data = {}
            data['end_time'] = datetime.datetime.fromtimestamp( int(b.data['last_chk']) ).strftime('%Y-%m-%d %H:%M:%S')
            data['expired'] = '1' 

            logger.info("[glpidb] host check data: %s", data)
            where_clause = {'items_id': service_cache['items_id'], 'itemtype': service_cache['itemtype']}
            query = self.db_backend.create_update_query('glpi_plugin_monitoring_acknowledges', data, where_clause)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)
        
    ## Update Shinken all hosts/services state
    def record_shinken_state(self, hostname, service, b):
        # Insert/update shinken state table
        # Select perfdata regexp
        exists = None
        query = "SELECT COUNT(*) AS nbRecords FROM `glpi_plugin_monitoring_shinken_states` WHERE hostname='%s' AND service='%s';" % (hostname, service)
        try:
            self.db_backend.execute_query(query)
            res = self.db_backend.fetchone()
            exists = True if res[0] > 0 else False
        except Exception as exp:
            # No more table update because table does not exist or is bad formed ...
            self.update_shinken_state = False
            logger.error("[glpidb] error '%s' when executing query: %s", exp, query)
            
        data = {}
        data['hostname'] = hostname
        data['service'] = service
        data['state'] = b.data['state_id']
        data['state_type'] = b.data['state_type']
        data['last_output'] = ("%s \n %s", b.data['output'], b.data['long_output']) if (len(b.data['long_output']) > 0) else b.data['output']
        data['last_check'] = datetime.datetime.fromtimestamp( int(b.data['last_chk']) ).strftime('%Y-%m-%d %H:%M:%S')
        data['last_perfdata'] = b.data['perf_data']
        data['is_ack'] = '1' if b.data['problem_has_been_acknowledged'] else '0'
        
        if exists:
            where_clause = {'hostname': hostname, 'service': service}
            query = self.db_backend.create_update_query('glpi_plugin_monitoring_shinken_states', data, where_clause)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)
        else:
            query = self.db_backend.create_insert_query('glpi_plugin_monitoring_shinken_states', data)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)
    
    def main(self):
        self.set_proctitle(self.name)
        self.set_exit_handler()
        while not self.interrupted:
            logger.debug("[glpidb] queue length: %s", self.to_q.qsize())
            start = time.time()
            l = self.to_q.get()
            for b in l:
                b.prepare()
                self.manage_brok(b)

            logger.debug("[glpidb] time to manage %s broks (%d secs)", len(l), time.time() - start)
