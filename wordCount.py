text_file = sc.textFile("hdfs://tmp/maria_dev/data/20000_gutenberg.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs:/tmp/maria_dev/out8/wordcout_output.txt")