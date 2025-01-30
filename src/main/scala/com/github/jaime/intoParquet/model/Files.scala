/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

/** Abstraction over files names so doesn't depend on Seqs*/
class Files(_items: Seq[String]) {
    def items: List[String] = _items.toList

    override def toString: String = items.mkString("; ")
}
