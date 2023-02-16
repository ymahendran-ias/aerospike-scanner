## About the Project: 

This project is designed to be a quick-and-dirty command-line utility to analyze two different aerospike-clusters for a given namespace-set and output the keys that are present in one (culprit) but not the other (kosher).

## Why did I create this project? 

OPS team encountered an aerospike cluster size issue, specifically in the JP region/cluster. 

As it stands we only issue writes to the VA cluster and have a master-slave setup for records to be replicated over to the other clusters.  
So ideally, all clusters should have the ~same data and size, except in reality one of the sets for the namespace in JP cluster was 30% over AU cluster. 

Since `aql` and `asadm` commands didn't provide enough info to get to the bottom of the issue, I ended up creating this full-cluster-scan tool that can: 
* pull the keys on all objects from any two clusters pointed to 
* compare the keys and emit object keys that are found only in problem-cluster and not in kosher-cluster

## To understand this project, args, setup better:
Please checkout the [ScannerJobFullIntegrationTest](src/test/groovy/com/truecube/aerospikescanner/ScannerJobFullIntegrationTest.groovy) full-integration testcase that goes through the entire setup (uses test-containers, groovy, spock)

## How to run this project: 
* git clone this project 
* Using IntelliJ: 
  * import the project into IntelliJ
  * Run the ScannerJob.java class with the arguments listed in the arg section
  * Ensure you adjust the memory settings appropriately as well: 
    * for eg: to scan a 38Million object cluster, (since the data is pulled locally and compared in memory) I needed to set the VM argument to `-Xmx10512m`

## Arguments
| Short Argument | LongForm Arg             | Description                                                                                                             | Example                        |
|----------------|--------------------------|-------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| -ccn,          | --culpritCluster.name    | name for the culprit cluster - sub-directories will be created under this name                                          | `-ccn japan`                   |
| -cServer       | --culpritServer          | hostname of the server that belongs to culprit cluster                                                                  | `-cServer host.net`            |
| -cPort         | --culpritPort            | port of the server that belongs to culprit cluster                                                                      | `-cPort 3000`                  |
| -csUser        | --culpritServer.user     | username to read from culprit cluster                                                                                   | `-csUser uName`                |
| -csPwd         | --culpritServer.password | password to read from culprit cluster                                                                                   | `-csPwd password`              |
| -kcn,          | --kosherCluster.name     | name for the kosher cluster - sub-directories will be created under this name                                           | `-kcn auz`                     |
| -kServer       | --kosherServer           | hostname of the server that belongs to kosher cluster                                                                   | `-kServer host2.net`           |
| -kPort         | --kosherPort             | port of the server that belongs to kosher cluster                                                                       | `-kPort 3000`                  |
| -ksUser        | --kosherServer.user      | username to read from culprit cluster                                                                                   | `-ksUser uName`                |
| -ksPwd         | --kosherServer.password  | password to read from culprit cluster                                                                                   | `-ksPwd password`              |
| -namespace     | --namespace              | namespace to read from aerospike                                                                                        | `-namespace product`           |
| -kSet          | --kosherSet              | set to read from kosher aerospike                                                                                       | `-kSet purchase_details`       |
| -cSet          | --culpritSet             | set to read from culprit aerospike                                                                                      | `-cSet purchase_details`       |
| -skipCompare   | --skipDataCompare        | set this to true if you just want the keys from both clusters to be dumped into files and skip the keys comparison part | `-skipCompare true`            |
| -skipFetch     | --skipDataFetch          | set this to true if you already have the keys dumped to a file and just want to do the data comparison                  | `-skipFetch true`              |
| -wd            | --workingDir             | Local Directory used to persist keys and outputs                                                                        | `-wd /Users/jdoe/Desktop/scan` |

## Example arg list: 
* ensure that you set your run with good memory if you are running data-compare using this vm arg: `-Xmx10512m`
* to run data fetch and data compare: `-ccn jp -cServer host1.net -cPort 3000 -csUser uName -csPwd pass -kServer host2.net -kPort 3000 -ksUser uName -ksPwd pass -kcn au -namespace product -kSet purchase_details -cSet purchase_details -wd /Users/jdoe/Desktop/autocheck`
* to run just data fetch and skip data compare: `-ccn jp -cServer host1.net -cPort 3000 -csUser uName -csPwd pass -kServer host2.net -kPort 3000 -ksUser uName -ksPwd pass -kcn au -namespace product -kSet purchase_details -cSet purchase_details -wd /Users/jdoe/Desktop/autocheck -skipCompare true`
* to run just data compare (in case you already have fetched data): `-ccn jp -cServer host1.net -cPort 3000 -csUser uName -csPwd pass -kServer host2.net -kPort 3000 -ksUser uName -ksPwd pass -kcn au -namespace product -kSet purchase_details -cSet purchase_details -wd /Users/jdoe/Desktop/autocheck -skipFetch true`

# License

This project is MIT Licensed.  See [`COPYING.txt`](COPYING.txt) and
[`LICENSE.txt`](LICENSE.txt) for details.