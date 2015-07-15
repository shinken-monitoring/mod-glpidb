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
#import MySQLdb


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
        self.update_availability = bool(getattr(modconf, 'update_availability', '0')=='1')
        self.update_shinken_state = bool(getattr(modconf, 'update_shinken_state', '0')=='1')
        self.update_services_events = bool(getattr(modconf, 'update_services_events', '0')=='1')
        self.update_hosts = bool(getattr(modconf, 'update_hosts', '0')=='1')
        self.update_services = bool(getattr(modconf, 'update_services', '0')=='1')
        self.update_acknowledges = bool(getattr(modconf, 'update_acknowledges', '0')=='1')
        logger.info("[glpidb] updating availability: %s", self.update_availability)
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

            # Update availability
            if self.update_availability:
                self.record_availability(host_name, '', b)

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

            # Update availability
            if self.update_availability:
                self.record_availability(host_name, service_description, b)

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
        logger.debug("[glpidb] record host check result: %s: %s", host_name, b.data)

        # Escape SQL fields ...
        # b.data['output'] = MySQLdb.escape_string(b.data['output'])
        # b.data['long_output'] = MySQLdb.escape_string(b.data['long_output'])
        # b.data['perf_data'] = MySQLdb.escape_string(b.data['perf_data'])

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
        #if self.update_acknowledges and b.data['state_id'] == 0 and b.data['last_state_id'] != 0:
        # Update acknowledge table if host is UP
        if self.update_acknowledges and b.data['state_id'] == 0:
            data = {}
            data['end_time'] = datetime.datetime.fromtimestamp( int(b.data['last_chk']) ).strftime('%Y-%m-%d %H:%M:%S')
            data['expired'] = '1'

            where_clause = {'items_id': host_cache['items_id'], 'itemtype': "PluginMonitoringHost"}
            query = self.db_backend.create_update_query('glpi_plugin_monitoring_acknowledges', data, where_clause)
            logger.debug("[glpidb] acknowledge query: %s", query)
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

        # Escape SQL fields ...
        # b.data['output'] = MySQLdb.escape_string(b.data['output'])
        # b.data['long_output'] = MySQLdb.escape_string(b.data['long_output'])
        # b.data['perf_data'] = MySQLdb.escape_string(b.data['perf_data'])

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

        # Update acknowledge table if service becomes OK
        #if self.update_acknowledges and b.data['state_id'] == 0 and b.data['last_state_id'] != 0:
        # Update acknowledge table if service is OK
        if self.update_acknowledges and b.data['state_id'] == 0:
            data = {}
            data['end_time'] = datetime.datetime.fromtimestamp( int(b.data['last_chk']) ).strftime('%Y-%m-%d %H:%M:%S')
            data['expired'] = '1'

            where_clause = {'items_id': service_cache['items_id'], 'itemtype': service_cache['itemtype']}
            query = self.db_backend.create_update_query('glpi_plugin_monitoring_acknowledges', data, where_clause)
            logger.debug("[glpidb] acknowledge query: %s", query)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)

    ## Update Shinken all hosts/services state
    def record_shinken_state(self, hostname, service, b):
        # Insert/update in shinken state table
        logger.debug("[glpidb] record shinken state: %s/%s: %s", hostname, service, b.data)

        # Test if record still exists
        exists = None
        query = "SELECT COUNT(*) AS nbRecords FROM `glpi_plugin_monitoring_shinkenstates` WHERE hostname='%s' AND service='%s';" % (hostname, service)
        try:
            self.db_backend.execute_query(query)
            res = self.db_backend.fetchone()
            exists = True if res[0] > 0 else False
        except Exception as exp:
            # No more table update because table does not exist or is bad formed ...
            self.update_shinken_state = False
            logger.error("[glpidb] error '%s' when executing query: %s", exp, query)

        # Escape SQL fields ...
        # b.data['output'] = MySQLdb.escape_string(b.data['output'])
        # b.data['long_output'] = MySQLdb.escape_string(b.data['long_output'])
        # b.data['perf_data'] = MySQLdb.escape_string(b.data['perf_data'])

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
            query = self.db_backend.create_update_query('glpi_plugin_monitoring_shinkenstates', data, where_clause)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)
        else:
            query = self.db_backend.create_insert_query('glpi_plugin_monitoring_shinkenstates', data)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)

    ## Update hosts/services availability
    def record_availability(self, hostname, service, b):
        # Insert/update in shinken state table
        logger.debug("[glpidb] record availability: %s/%s: %s", hostname, service, b.data)
        if hostname.startswith('sim'):
            logger.warning("[glpidb] record availability: %s/%s: %s", hostname, service, b.data)

        # Host check brok:
        # ----------------
        # {'last_time_unreachable': 0, 'last_problem_id': 1, 'check_type': 1, 'retry_interval': 1, 'last_event_id': 1, 'problem_has_been_acknowledged': False, 'last_state': 'DOWN', 'latency': 0, 'last_state_type': 'HARD', 'last_hard_state_change': 1433822140, 'last_time_up': 1433822140, 'percent_state_change': 0.0, 'state': 'UP', 'last_chk': 1433822138, 'last_state_id': 0, 'end_time': 0, 'timeout': 0, 'current_event_id': 1, 'execution_time': 0, 'start_time': 0, 'return_code': 0, 'state_type': 'HARD', 'output': '', 'in_checking': False, 'early_timeout': 0, 'in_scheduled_downtime': False, 'attempt': 1, 'state_type_id': 1, 'acknowledgement_type': 1, 'last_state_change': 1433822140.825969, 'last_time_down': 1433821584, 'instance_id': 0, 'long_output': '', 'current_problem_id': 0, 'host_name': 'sim-0003', 'check_interval': 60, 'state_id': 0, 'has_been_checked': 1, 'perf_data': u''}
        #
        # Interesting information ...
        # 'state_id': 0 / 'state': 'UP' / 'state_type': 'HARD'
        # 'last_state_id': 0 / 'last_state': 'UP' / 'last_state_type': 'HARD'
        # 'last_time_unreachable': 0 / 'last_time_up': 1433152221 / 'last_time_down': 0
        # 'last_chk': 1433152220 / 'last_state_change': 1431420780.184517
        # 'in_scheduled_downtime': False

        # Service check brok:
        # -------------------
        # {'last_problem_id': 0, 'check_type': 0, 'retry_interval': 2, 'last_event_id': 0, 'problem_has_been_acknowledged': False, 'last_time_critical': 0, 'last_time_warning': 0, 'end_time': 0, 'last_state': 'OK', 'latency': 0.2347090244293213, 'last_time_unknown': 0, 'last_state_type': 'HARD', 'last_hard_state_change': 1433736035, 'percent_state_change': 0.0, 'state': 'OK', 'last_chk': 1433785101, 'last_state_id': 0, 'host_name': u'shinken24', 'has_been_checked': 1, 'check_interval': 5, 'current_event_id': 0, 'execution_time': 0.062339067459106445, 'start_time': 0, 'return_code': 0, 'state_type': 'HARD', 'output': 'Ok : memory consumption is 37%', 'service_description': u'Memory', 'in_checking': False, 'early_timeout': 0, 'in_scheduled_downtime': False, 'attempt': 1, 'state_type_id': 1, 'acknowledgement_type': 1, 'last_state_change': 1433736035.927526, 'instance_id': 0, 'long_output': u'', 'current_problem_id': 0, 'last_time_ok': 1433785103, 'timeout': 0, 'state_id': 0, 'perf_data': u'cached=13%;;;0%;100% buffered=1%;;;0%;100% consumed=37%;80%;90%;0%;100% used=53%;;;0%;100% free=46%;;;0%;100% swap_used=0%;;;0%;100% swap_free=100%;;;0%;100% buffered_abs=36076KB;;;0KB;2058684KB used_abs=1094544KB;;;0KB;2058684KB cached_abs=284628KB;;;0KB;2058684KB consumed_abs=773840KB;;;0KB;2058684KB free_abs=964140KB;;;0KB;2058684KB total_abs=2058684KB;;;0KB;2058684KB swap_total=392188KB;;;0KB;392188KB swap_used=0KB;;;0KB;392188KB swap_free=392188KB;;;0KB;392188KB'}
        #
        # Interesting information ...
        # 'state_id': 0 / 'state': 'OK' / 'state_type': 'HARD'
        # 'last_state_id': 0 / 'last_state': 'OK' / 'last_state_type': 'HARD'
        # 'last_time_critical': 0 / 'last_time_warning': 0 / 'last_time_unknown': 0 / 'last_time_ok': 1433785103
        # 'last_chk': 1433785101 / 'last_state_change': 1433736035.927526
        # 'in_scheduled_downtime': False

        # Only for simulated hosts ...
        # if not hostname.startswith('sim'):
            # return

        # Only for host check ...
        if not service is '':
            return

        # Ignoring SOFT states ...
        if b.data['state_type_id']==0:
            logger.warning("[glpidb] record availability for: %s/%s, but no HARD state, ignoring ...", hostname, service)


        midnight = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        midnight_timestamp = time.mktime (midnight.timetuple())
        # Number of seconds today ...
        seconds_today = int(b.data['last_chk']) - midnight_timestamp
        # Number of seconds since state changed
        since_last_state = int(b.data['last_state_change']) - seconds_today
        # Scheduled downtime
        scheduled_downtime = bool(b.data['in_scheduled_downtime'])
        # Day
        day = datetime.date.today().strftime('%Y-%m-%d')

        # Database table
        # --------------
        # `hostname` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
        # `service` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
        # `day` DATE DEFAULT NULL,
        # `is_downtime` tinyint(1) DEFAULT '0',
        # `daily_0` int(6) DEFAULT '0',                 Up/Ok
        # `daily_1` int(6) DEFAULT '0',                 Down/Warning
        # `daily_2` int(6) DEFAULT '0',                 Unreachable/Critical
        # `daily_3` int(6) DEFAULT '0',                 Unknown
        # `daily_4` int(6) DEFAULT '86400',             Unchecked
        # `daily_9` int(6) DEFAULT '0',                 Downtime
        # --------------

        # Test if record for current day still exists
        exists = False
        res = None
        query = """SELECT id, hostname, service, day, is_downtime,
                    daily_0, daily_1, daily_2, daily_3, daily_4,
                    first_check_state, first_check_timestamp,
                    last_check_state, last_check_timestamp
                    FROM `glpi_plugin_monitoring_availabilities`
                    WHERE hostname='%s' AND service='%s' AND day='%s';""" % (hostname, service, day)
        try:
            self.db_backend.execute_query(query)
            res = self.db_backend.fetchone()
            logger.warning("[glpidb] record availability, select query result: %s", res)
                # (9L, 'sim-0001', '', datetime.date(2015, 6, 9), 0, 0L, 0L, 0L, 0L, 86400L, 1, 1433854693L, 1, 1433854693L)
            exists = True if res is not None else False
        except Exception as exp:
            # No more table update because table does not exist or is bad formed ...
            self.update_shinken_state = False
            logger.error("[glpidb] error '%s' when executing query: %s", exp, query)

        # Configure recorded data
        data = {}
        data['hostname'] = hostname
        data['service'] = service
        data['day'] = day
        data['is_downtime'] = '1' if bool(b.data['in_scheduled_downtime']) else '0'
        # All possible states are 0 seconds duration.
        data['daily_0'] = 0
        data['daily_1'] = 0
        data['daily_2'] = 0
        data['daily_3'] = 0
        data['daily_4'] = 0

        current_state = b.data['state']
        current_state_id = b.data['state_id']
        last_state = b.data['last_state']
        last_check_state = res[12] if res else 3
        last_check_timestamp = res[13] if res else midnight_timestamp
        since_last_state = 0
        logger.warning("[glpidb] current state: %s, last state: %s", current_state, last_state)

        # Host check
        if service=='':
            last_time_unreachable = b.data['last_time_unreachable']
            last_time_up = b.data['last_time_up']
            last_time_down = b.data['last_time_down']
            last_state_change = b.data['last_state_change']
            last_state_change = int(time.time())

            if current_state == 'UP':
                since_last_state = int(last_state_change - last_check_timestamp)

            elif current_state== 'UNREACHABLE':
                since_last_state = int(last_state_change - last_check_timestamp)

            elif current_state == 'DOWN':
                since_last_state = int(last_state_change - last_check_timestamp)

        # Service check
        # else:
            # To be implemented !!!
            # if hostname.startswith('sim'):
                # logger.warning("[glpidb] last_time_unknown: %d", b.data['last_time_unknown'])
                # logger.warning("[glpidb] last_time_ok: %d", b.data['last_time_ok'])
                # logger.warning("[glpidb] last_time_warning: %d", b.data['last_time_warning'])
                # logger.warning("[glpidb] last_time_critical: %d", b.data['last_time_critical'])

        # Update existing record
        if exists:
            data = {
                    'is_downtime': res[4],
                    'daily_0': res[5], 'daily_1': res[6], 'daily_2': res[7], 'daily_3': res[8], 'daily_4': res[9]
                    }

            logger.debug("[glpidb] current data: %s", data)

            # Update record
            if since_last_state > seconds_today:
                # Last state changed before today ...

                # Current state duration for all seconds of today
                data["daily_%d" % current_state_id] = seconds_today
            else:
                # Increase current state duration with seconds since last state
                data["daily_%d" % b.data['state_id']] += (since_last_state)

            # Unchecked state for all day duration minus all states duration
            data['daily_4'] = 86400
            for value in [ data['daily_0'], data['daily_1'], data['daily_2'], data['daily_3'] ]:
                data['daily_4'] -= value

            # Last check state and timestamp
            data['last_check_state'] = current_state_id
            data['last_check_timestamp'] = int(b.data['last_chk'])

            where_clause = {'hostname': hostname, 'service': service, 'day': day}
            query = self.db_backend.create_update_query('glpi_plugin_monitoring_availabilities', data, where_clause)
            logger.warning("[glpidb] record availability, update query: %s", query)
            try:
                self.db_backend.execute_query(query)
            except Exception as exp:
                logger.error("[glpidb] error '%s' when executing query: %s", exp, query)

        # Create record
        else:
            # First check state and timestamp
            data['first_check_state'] = current_state_id
            data['first_check_timestamp'] = int(b.data['last_chk'])

            # Last check state and timestamp
            data['last_check_state'] = current_state_id
            data['last_check_timestamp'] = int(b.data['last_chk'])

            # Ignore computed values because it is the first check received today!
            data['daily_4'] = 86400

            query = self.db_backend.create_insert_query('glpi_plugin_monitoring_availabilities', data)
            logger.warning("[glpidb] record availability, insert query: %s", query)
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
