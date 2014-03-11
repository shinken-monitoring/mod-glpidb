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
import sys


from shinken.basemodule import BaseModule
from shinken.log import logger

properties = {
    'daemons': ['broker'],
    'type': 'glpidb',
    'external': False,
}


# Called by the plugin manager to get a broker
def get_instance(plugin):
    logger.info("[GLPIdb broker] Get a glpidb data module for plugin %s" % plugin.get_name())
    instance = Glpidb_broker(plugin)
    return instance


# Class for the Glpidb Broker
# Get broks and puts them in GLPI database
class Glpidb_broker(BaseModule):
    def __init__(self, modconf):
        # Mapping for name of data, rename attributes and transform function
        self.mapping = {
           # Host
           'host_check_result': {
               'event': {'transform': None},
               'perf_data': {'transform': None},
               'long_output': {'transform': None},
               'state': {'transform': None},
               'latency': {'transform': None},
               'execution_time': {'transform': None},
               'state_type': {'transform': None},
               'host_name': {'transform': None},
               },
           # Service
           'service_check_result': {
               'event': {'transform': None},
               'perf_data': {'transform': None},
               'long_output': {'transform': None},
               'state': {'transform': None},
               'latency': {'transform': None},
               'execution_time': {'transform': None},
               'state_type': {'transform': None},
               'service_description': {'transform': None},
               'host_name': {'transform': None},
               }
           }
           
        BaseModule.__init__(self, modconf)

        self.cache_host_id = {}
        self.cache_host_items_id = {}
        self.cache_host_itemtype = {}
        self.cache_service_items_id = {}
        self.cache_service_itemtype = {}
        try:
            self.host = getattr(modconf, 'host', '127.0.0.1')
            self.user = getattr(modconf, 'user', 'shinken')
            self.password = getattr(modconf, 'password', 'shinken')
            self.database = getattr(modconf, 'database', 'glpidb')
            self.character_set = getattr(modconf, 'character_set', 'utf8')
        except AttributeError:
            logger.error("[GLPIdb Broker] The module is missing a property, check module declaration in glpidb.cfg")
            raise

    # Called by Broker so we can do init stuff
    # TODO: add conf param to get pass with init
    # Conf from arbiter!
    def init(self):
        from shinken.db_mysql import DBMysql
        logger.info("[GLPIdb Broker] Creating a mysql backend : %s (%s)" % (self.host, self.database))
        self.db_backend = DBMysql(self.host, self.user, self.password, self.database, self.character_set)

        logger.info("[GLPIdb Broker] Connecting to database ...")
        self.db_backend.connect_database()
        logger.info("[GLPIdb Broker] Connected")

    def manage_initial_host_status_brok(self, b):
        data = b.data
        logger.debug("[GLPIdb Broker] Initial host status : %s" % str(data))

        try:
            self.cache_host_id[data['host_name']] = data['customs']['_HOSTID']
        except:
            self.cache_host_id[data['host_name']] = -1
            logger.debug("[GLPIdb Broker] no custom _HOSTID for %s" % data['host_name'])
            
        try:
            self.cache_host_itemtype[data['host_name']] = data['customs']['_ITEMTYPE']
        except:
            logger.debug("[GLPIdb Broker] no custom _ITEMTYPE")
            
        try:
            self.cache_host_items_id[data['host_name']] = data['customs']['_ITEMSID']
        except:
            logger.debug("[GLPIdb Broker] no custom _ITEMSID")

    def manage_initial_service_status_brok(self, b):
        data = b.data
        logger.debug("[GLPIdb Broker] Initial service status : %s" % str(data))
        
        if 'host_name' in b.data:
            if not b.data['host_name'] in self.cache_host_id or self.cache_host_id[data['host_name']] == -1:
                logger.debug("GLPIdb: host is not defined in Glpi : %s." % b.data['host_name'])
                return

        if not data['host_name'] in self.cache_service_itemtype:
            self.cache_service_itemtype[data['host_name']] = {}
        try:
            self.cache_service_itemtype[data['host_name']][data['service_description']] = data['customs']['_ITEMTYPE']
        except:
            logger.debug("[GLPIdb Broker] no custom _ITEMTYPE")
            
        if not data['host_name'] in self.cache_service_items_id:
            self.cache_service_items_id[data['host_name']] = {}
        try:
            self.cache_service_items_id[data['host_name']][data['service_description']] = data['customs']['_ITEMSID']
        except:
            logger.debug("[GLPIdb Broker] no custom _ITEMSID")

    def preprocess(self, type, brok, checkst):
        new_brok = copy.deepcopy(brok)

        if not 'initial' in type:
            if 'host_name' in brok.data:
                if not brok.data['host_name'] in self.cache_host_id or self.cache_host_id[brok.data['host_name']] == -1:
                    logger.debug("GLPIdb: host is not defined in Glpi : %s / %s." % (type, brok.data['host_name']))
                    return

        # Only preprocess if we can apply a mapping
        if type in self.mapping:
            # logger.debug("[GLPIdb Broker] brok data: %s" % str(brok.data))
            if 'service_description' in new_brok.data:
                new_brok.data['id'] = self.cache_service_items_id[brok.data['host_name']][brok.data['service_description']]
                new_brok.data['items_id'] = self.cache_service_items_id[brok.data['host_name']][brok.data['service_description']]
                new_brok.data['itemtype'] = self.cache_service_itemtype[brok.data['host_name']][brok.data['service_description']]
            else:
                new_brok.data['host_id'] = self.cache_host_id[brok.data['host_name']]
                new_brok.data['items_id'] = self.cache_host_items_id[brok.data['host_name']]
                new_brok.data['itemtype'] = self.cache_host_itemtype[brok.data['host_name']]
                
            new_brok.data['event'] = brok.data['output']
            
            to_del = []
            to_add = []
            mapping = self.mapping[brok.type]
            for prop in new_brok.data:
            # ex: 'name': 'program_start_time', 'transform'
                if prop in mapping:
                    # logger.debug("[GLPIdb Broker] Got a prop to change: %s" % prop)
                    val = new_brok.data[prop]
                    if mapping[prop]['transform'] is not None:
                        # logger.debug("[GLPIdb Broker] Call function for type %s and prop %s" % (type, prop))
                        f = mapping[prop]['transform']
                        val = f(val)
                    name = prop
                    if 'name' in mapping[prop]:
                        name = mapping[prop]['name']
                    to_add.append((name, val))
                    to_del.append(prop)
                else:
                    to_del.append(prop)
            for prop in to_del:
                del new_brok.data[prop]
            for (name, val) in to_add:
                new_brok.data[name] = val
        else:
            # print "No preprocess type", brok.type
            if 'service_description' in brok.data:
                self.manage_initial_service_status_brok(brok)
            else:
                self.manage_initial_host_status_brok(brok)
            # print brok.data
        return new_brok

    # Get a brok, parse it, and put in in database
    # We call functions like manage_ TYPEOFBROK _brok that return us queries
    def manage_brok(self, b):
        # if b.type != 'log':
            # logger.warning("[GLPIdb Broker] manage_brok %s" % (b.type))
        
        type = b.type
        
        # Used to generate cache
        manager = 'manage_' + type + '_brok'
        if hasattr(self, manager):
            new_b = self.preprocess(type, b, 0)
        
        # Update service/host in GLPI DB
        manager = 'manage_' + type + '_update_brok'
        if hasattr(self, manager):
            if 'host_name' in b.data:
                if not b.data['host_name'] in self.cache_host_id or self.cache_host_id[b.data['host_name']] == -1:
                    logger.debug("[GLPIdb Broker] host is not defined in Glpi : %s." % b.data['host_name'])
                    return
                    
            new_b = self.preprocess(type, b, 0)
            f = getattr(self, manager)
            queries = f(new_b)
            # Ok, we've got queries, now: run them!
            for q in queries:
                logger.info("[GLPIdb Broker] MySQL query %s" % q)
                self.db_backend.execute_query(q)
        
        # Add event + perfdata in GLPI DB
        manager = 'manage_' + type + '_addevent_brok'
        if hasattr(self, manager):
            if 'host_name' in b.data:
                if not b.data['host_name'] in self.cache_host_id or self.cache_host_id[b.data['host_name']] == -1:
                    logger.debug("[GLPIdb Broker] host is not defined in Glpi : %s." % b.data['host_name'])
                    return
                    
            new_b = self.preprocess(type, b, '1')
            if 'host_name' in new_b.data:
                if 'service_description' not in new_b.data:
                    return
            f = getattr(self, manager)
            queries = f(new_b)
            # Ok, we've got queries, now: run them!
            for q in queries:
                logger.info("[GLPIdb Broker] MySQL query %s" % q)
                self.db_backend.execute_query(q)

    ## Host result
    ## def manage_host_check_result_brok(self, b):
    ##     logger.info("[GLPIdb Broker] data in DB %s " % b)
    ##     b.data['date'] = time.strftime('%Y-%m-%d %H:%M:%S')
    ##     query = self.db_backend.create_insert_query('glpi_plugin_monitoring_serviceevents', b.data)
    ##     return [query]


    ## Host result
    def manage_host_check_result_update_brok(self, b):
        if 'host_name' in b.data:
            if not b.data['host_name'] in self.cache_host_id or self.cache_host_id[b.data['host_name']] == -1:
                logger.debug("[GLPIdb Broker] host is not defined in Glpi : %s." % b.data['host_name'])
                return

        logger.info("[GLPIdb Broker] Host data in DB %s " % b)
        
        new_data = copy.deepcopy(b.data)
        new_data['last_check'] = time.strftime('%Y-%m-%d %H:%M:%S')
        new_data['host_id'] = self.cache_host_id[new_data['host_name']]
        new_data['items_id'] = self.cache_host_items_id[new_data['host_name']]
        new_data['itemtype'] = self.cache_host_itemtype[new_data['host_name']]
        # del new_data['perf_data']
        # del new_data['output']
        # del new_data['latency']
        # del new_data['execution_time']
        new_data['event'] = "%s \n %s" % (new_data['event'], new_data['long_output'])
        del new_data['long_output']
        del new_data['host_name']
        if (new_data['state'] == 'UP') and new_data['host_id'] != -1:
            new_data['is_acknowledged'] = 0

            queryAck = "UPDATE glpi_plugin_monitoring_acknowledges set end_time='%s', expired='1' WHERE expired='0' and items_id='%s' and itemtype='%s'" % (time.strftime('%Y-%m-%d %H:%M:%S'), new_data['host_id'], "PluginMonitoringHost")
            logger.debug("[GLPIdb Broker] Acknowledge host query %s " % queryAck)
            
        del new_data['host_id']
        # del new_data['items_id']
        # del new_data['itemtype']
        where_clause = {'items_id': new_data['items_id'], 'itemtype': new_data['itemtype']}
        query = self.db_backend.create_update_query('glpi_plugin_monitoring_hosts', new_data, where_clause)
        try:
            return [query, queryAck]
        except NameError:
            return [query]
            
    # Add service event (state + perfdata)
    def manage_service_check_result_addevent_brok(self, b):
        # if 'host_name' in b.data:
            # if not b.data['host_name'] in self.cache_host_id or self.cache_host_id[data['host_name']] == -1:
                # logger.info("[GLPIdb Broker] host is not defined in Glpi : %s." % b.data['host_name'])
                # return

        if not b.data['host_name'] in self.cache_service_itemtype or not b.data['service_description'] in self.cache_service_itemtype[b.data['host_name']]:
            logger.debug("[GLPIdb Broker] cache not ready ")
            return ''

        logger.debug("[GLPIdb Broker] Data in DB %s" % b)

        b.data['date'] = time.strftime('%Y-%m-%d %H:%M:%S')
        b.data['plugin_monitoring_services_id'] = self.cache_service_items_id[b.data['host_name']][b.data['service_description']]
        del b.data['host_name']
        del b.data['service_description']
        b.data['event'] = "%s \n %s" % (b.data['event'], b.data['long_output'])
        del b.data['long_output']
        # del b.data['output']
        query = self.db_backend.create_insert_query('glpi_plugin_monitoring_serviceevents', b.data)
        return [query]

    # Update service 
    def manage_service_check_result_update_brok(self, b):
        """If a host is defined locally (in shinken) and not in GLPI,
           we must not edit GLPI datas!
        """
        if not b.data['host_name'] in self.cache_service_itemtype or not b.data['service_description'] in self.cache_service_itemtype[b.data['host_name']]:
            logger.debug("[GLPIdb Broker] host is not defined in Glpi.")
            return []

        logger.info("[GLPIdb Broker] Service data in DB %s " % str(b.data))
        
        new_data = copy.deepcopy(b.data)
        new_data['last_check'] = time.strftime('%Y-%m-%d %H:%M:%S')
        new_data['items_id'] = self.cache_service_items_id[new_data['host_name']][new_data['service_description']]
        new_data['itemtype'] = self.cache_service_itemtype[new_data['host_name']][new_data['service_description']]
        new_data['event'] = "%s \n %s" % (new_data['event'], new_data['long_output'])
        del new_data['long_output']
        del new_data['perf_data']
        # del new_data['output']
        del new_data['latency']
        del new_data['execution_time']
        if (new_data['state'] == 'OK'):
            new_data['is_acknowledged'] = 0
            
            queryAck = "UPDATE glpi_plugin_monitoring_acknowledges set end_time='%s', expired='1' WHERE expired='0' and items_id='%s' and itemtype='%s'" % (time.strftime('%Y-%m-%d %H:%M:%S'), new_data['items_id'], "PluginMonitoring%s" % new_data['itemtype'])
            logger.debug("[GLPIdb Broker] Acknowledge service query %s " % queryAck)
            
        new_data['id'] = self.cache_service_items_id[b.data['host_name']][b.data['service_description']]
        del new_data['host_name']
        del new_data['service_description']
        table = 'glpi_plugin_monitoring_services'
        if self.cache_service_itemtype[b.data['host_name']][b.data['service_description']] == 'ServiceCatalog':
            table = 'glpi_plugin_monitoring_servicescatalogs'

        del new_data['items_id']
        del new_data['itemtype']
        where_clause = {'id': new_data['id']}
        query = self.db_backend.create_update_query(table, new_data, where_clause)
        try:
            return [query, queryAck]
        except NameError:
            return [query]
