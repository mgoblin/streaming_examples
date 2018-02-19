package ru.sbt.utils

import org.apache.flink.api.common.typeinfo.TypeInformation

case class CSVParams(
    tableName: String,
    path: String,
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]],
    fieldDelim: String,
    rowDelim: String,
    quoteCharacter: Character,
    ignoreFirstLine: Boolean,
    ignoreComments: String,
    lenient: Boolean
)