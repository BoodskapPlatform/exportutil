# Boodskap IoT Platform's Export / Import Utility

#### Legends

Throughout the export / import, you will have to euqate the below arguments matching your environment

- **cassandra_ip_address**
    - Your cassandra server's ip address
- **cluster_name**
    - Your elasticsearch's configured cluster name
- **node_name**
    - Your elasticsearch's configured node name
- **node_ip_address**
    - Your elasticsearch node's ip address


#### Exporting from Cassandra Database

- Login as cassandra user
- Copy the **export.cql** under the directory you want to export
- Create the below folder structure under the directory you want to export

```bash
mkdir -p ./data/cassandra/dump/
```

- Use the cqlsh utility to export

```bash
$HOME/bin/cqlsh cassandra_ip_address -f export.cql
```

Before we can import data, we need to curate the exported dumps, the cqlsh utility just dumps out everything in one single csv file per table. 


#### Curating the Exported Dumps

You can either curate all of the data, or only a few domain data

- Curate everything
    - java -jar exportutil-1.0.0.jar -t **curate** -c cluster-name -n node_name -h node_ip_address -f file -d
- Curate domain data
    - java -jar exportutil-1.0.0.jar -t **curate** -c cluster-name -n node_name -h node_ip_address -f file -d comma_separated_domain_keys

#### Importing to Cassandra Database

The curator utility should have created individual folders for each domains, you can import them individually

```bash
cd ./data/cassandra/curated/domain_key
$HOME/bin/cqlsh cassandra_ip_address -f import.cql
```

Here **domain_key** should be replaced with your domain key 

You can do a directory listing under **./data/cassandra/curated/** to see the curated domains keys


#### Exporting from Elastic Search

While importing your can either import all of the data, or only a few domain data 

- Export everything
    - java -jar exportutil-1.0.0.jar -t **export** -c cluster-name -n node_name -h node_ip_address -m -r -d
        - If you want to export everything into the filesystem instead of a compressed DB file, you can add a flag **-f file**
        - **Note**, if you have exported as filessystem files, then remember to import using the same flag **-f file**
- Exporting domain data
    - java -jar exportutil-1.0.0.jar -t **export** -c cluster-name -n node_name -h node_ip_address -f file -m -r -d comma_separated_domain_keys


#### Importing to Elastic Search

While importing your can either import all of the data, or only a few domain data 

- Importing everything
    - java -jar exportutil-1.0.0.jar -t **import** -c cluster-name -n node_name -h node_ip_address -d
- Importing domain data
    - java -jar exportutil-1.0.0.jar -t **import** -c cluster-name -n node_name -h node_ip_address -d comma_separated_domain_keys


