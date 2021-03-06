<a href='https://travis-ci.org/shinken-monitoring/mod-glpidb'><img src='https://api.travis-ci.org/shinken-monitoring/mod-glpidb.svg?branch=master' alt='Travis Build'></a>
=================================
Shinken GLPI integration - GlpiDB
=================================

Shinken module for exporting data to GLPI DB for plugin monitoring module

This version works with plugin monitoring for GLPI from version 0.84+1.1.
For GLPI plugin Monitoring, see https://github.com/ddurieux/glpi_monitoring

For people not familiar with GLPI, it is an Open-Source CMDB. Applicable to servers, routers, printers or anything you want for that matter. It is also a help-desk tool. GLPI also integrates with tools like FusionInventory for IT inventory management.


Requirements
=============

  - Compatible version of GLPI Shinken module and GLPI version

      The current version needs:
       - plugin monitoring 0.84+1.1 for GLPI.
       - plugin WebServices for GLPI

       See https://forge.indepnet.net to get the plugins.


  - Python libraries

      Please install `python-mysqldb` to gain accesss to Mysql DB from Python.



Enabling GLPIdb Shinken module
==============================

To use the glpidb module you must declare it in your broker configuration.

```

  define broker {
      ...

      modules    	 ..., glipdb

  }
```

The module configuration is defined in the file: glpidb.cfg.

Default configuration needs to be tuned up to your Glpi configuration.

First you need to define database configuration. Set host, port, database, username and password for your current configuration.

Default module behaviour is to update all possible tables used by the Monitoring plugin, but this may be tuned up with configuration parameters.

The module updates :

   - services events, to keep a log of all events
   - hosts, to track current hosts states
   - services, to track current services states
   - acknowledges, to update acknowledges when host/service recovers

The Shinken state maintains a table indexed upon host/service. This table stores last host/services states even for hosts that are not configured from Glpi database.

The update_shinken_state should be False if you do not have a recent Glpi Monitoring version (at least 0.85+1.1). In any case, this feature will auto disable if the corresponding table does not exist in your Glpi database.

The module manages an internal queue for updating the service_events table. A bulk insertion is
periodically called (commit_period), this method prepares a bunch of queued insertions (max.
commit_volume) to insert them in the DB in one INSERT query.


Default configuration file is as is :
```

## Module:      glpidb
## Loaded by:   Broker
# Export data to the GLPI database from a Shinken broker.
define module {
    module_name     glpidb
    module_type     glpidb
    host            localhost   ; GLPI database server name or IP
    port            3306
    database        glpidb      ; Database name
    user            shinken     ; Database user
    password        shinken

    # Update Shinken state table : hostname/service
    update_shinken_state         1
    # Update services events table : log of all events
    update_services_events       1
    # Update hosts state table
    update_hosts                 1
    # Update services state table
    update_services              1
    # Update acknowledges table
    update_acknowledges          0
    # Update availability table
    update_availability          0

    # Every commit_period seconds, up to commit_volume events are inserted into the Glpi DB ...
    commit_period   10
    commit_volume   100

    # Every db_test_period seconds, the database connection is tested if connection has been lost ...
    db_test_period  30
}
```
