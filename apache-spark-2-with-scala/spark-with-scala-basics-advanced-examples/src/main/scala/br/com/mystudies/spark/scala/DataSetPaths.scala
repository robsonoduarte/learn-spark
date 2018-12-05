package br.com.mystudies.spark.scala

object DataSetsPaths {
  def path(name: String) = getClass.getResource(s"/datasets/$name").getPath
}