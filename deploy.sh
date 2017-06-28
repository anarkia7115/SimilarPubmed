#scp /home/shawn/git/PubmedWordGame/SimilarPubmed/target/scala-2.11/SimilarPubmed-assembly-1.0.jar gcbi:~/jars/
scp -r /home/shawn/git/PubmedWordGame/SimilarPubmed/src/main/python/* soldier1:/home/gcbi/workspace/calcScores
scp -r /home/shawn/git/PubmedWordGame/SimilarPubmed/src/main/cpp/ soldier1:/home/gcbi/workspace/calcScores/src/main/
scp /home/shawn/git/PubmedWordGame/SimilarPubmed/Makefile soldier1:/home/gcbi/workspace/calcScores/
