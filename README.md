## About the Project: 

This project is designed to be a quick-and-dirty command-line utility to analyze two different aerospike-clusters for a given namespace-set and output the keys that are present in one (culprit) but not the other (kosher).

## Why did I create this project? 

OPS team encountered an aerospike cluster size issu, specifically in the JP region/cluster. 

As it stands we only issue writes to the VA cluster and have a master-slave setup for records to be replicated over to the other clusters.  
So ideally, all clusters should have the same data and size, except for `scores` namespace JP cluster was 30% over AU cluster. 

Since aql and asadm commands didn't provide enough info to get to the bottom of the issue, created this full cluster scan tool that can: 
* pull the keys on all objects from both clusters 
* compare the keys and emit files that are found extra in problem cluster comopared to the kosher cluster

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
| -ccn,          | --culpritCluster.name    | name for the culprit cluster - sub-directories will be created under this name                                          | -ccn japan                     |
| -cServer       | --culpritServer          | hostname of the server that belongs to culprit cluster                                                                  | -cServer aero01.jp.303net.net  |
| -csUser        | --culpritServer.user     | username to read from culprit cluster                                                                                   | -csUser scores-ro              |
| -csPwd         | --culpritServer.password | password to read from culprit cluster                                                                                   | -csPwd password                |
| -kcn,          | --kosherCluster.name     | name for the kosher cluster - sub-directories will be created under this name                                           | -kcn auz                       |
| -kServer       | --kosherServer           | hostname of the server that belongs to kosher cluster                                                                   | -kServer aero01.au.303net.net  |
| -ksUser        | --kosherServer.user      | username to read from culprit cluster                                                                                   | -ksUser scores-ro              |
| -ksPwd         | --kosherServer.password  | password to read from culprit cluster                                                                                   | -ksPwd password                |
| -namespace     | --namespace              | namespace to read from aerospike                                                                                        | -namespace scores              |
| -set           | --set                    | set to read from aerospike                                                                                              | -set cat_scores                |
| -skipCompare   | --skipDataCompare        | set this to true if you just want the keys from both clusters to be dumped into files and skip the keys comparison part | -skipCompare true              |
| -skipFetch     | --skipDataFetch          | set this to true if you already have the keys dumped to a file and just want to do the data comparison                  | -skipFetch true                |
| -wd            | --workingDir             | Local Directory used to persist keys and outputs                                                                        | -wd /Users/jdoe/Desktop/scores |

## Example arg list: 
* `-ccn jp -cServer aero01.jp.303net.net -csUser scores-ro -csPwd pass -kServer aero01.au.303net.net -ksUser scores-ro -ksPwd pass -kcn au -namespace scores -set cat_scores -wd /Users/jdoe/Desktop/autocheck`
* vm arg: `-Xmx10512m`