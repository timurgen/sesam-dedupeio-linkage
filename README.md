# sesam-dedupeio-linkage
Record linkage powered by Dedupe.io for Sesam.io applications POC

Simple script to link possible duplicates in two different data sets for Sesam.io powered applications.
Result data set may be stored back to Sesam target pipe or just written to console.


(for small sets (up to 10 000 elements) as in memory processing, or large if you have a lot of RAM)
Usage: This script takes data from 2 published datasets and sends result to target dataset
or outputs it if target is not  declared.
Running this script as service on a small size Sesam (with GDPR setup) node will most probably cause out of memory,
even for small data sets.

environment: {  
    JWT:            <- jwt for respective Sesam node  
    KEYS            <- which properties of data will be used for matching (must be same in both datasets)  
    INSTANCE        <- URL for Sesam node  
    SOURCE1         <- name of first, master data set  
    SOURCE2         <- name of second, slave data set  
    TARGET          <- name of target data set  
    SETTINGS_FILE   <- trained model for Dedupe.io engine, if None then active training will be performed.  
                        May be path to file on local system or URL  
}  

