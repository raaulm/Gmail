# Gmail

This code makes the download of Gmail accounts by reading from a config file (that contains user and password), writes the e-mails in files and makes a put in HDFS. It is also download attachments. The process occurs in parallel so like this it can be distributed for each node on the cluster.
