package br.com.mystudies.spark.scala;

import static java.nio.file.Files.lines;
import static java.nio.file.Paths.get;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;

public class RatingsCounterJava {

	public static void main(String[] args) throws IOException {
		lines(get("F:/spark-scala-course/ml-100k/u.data"))
			.map(s -> s.split("\t")[2])
			.collect(groupingBy(s -> s, counting()))
			.forEach((x,y) -> System.out.println(x + " " + y));
	}

}
