# yn_similarity

This is a simple program for calculating similarity in spark cluster.

Do the following to run the program.

(1)Install spark cluster. I setup/run one spark master and two workers on my laptop as running environment.

(2)use virtual python environment. I use anaconda for it.

(3)create a 'data' subdirectory and put all .txt files as testing files. 

   I use f1.txt. f2.txt and f3.txt as all testing files.
   
(4)running command line:

   $SPARK_HOME/bin/spark-submit --master spark://masterIP:7077 yn_similarity.py.
   
   Here masterIP is the IP of your spark master. You can see the final results from the display screen.
   
(5)if you want to check all final results, use the following like this below

   SPARK_HOME/bin/spark-submit --master spark://masterIP:7077 yn_similarity.py > yn_final_results.txt
   
   yn_final_results.txt is the final file that contains all jaccard similarity of any pairs in f1.txt, f2.txt and f3.txt
   
(6)it's easy to use other similarity to replace jaccard similarity in code.

(7)In product system, it's better to use No-SQL database ( like cassandra or MongoDB, or others )
