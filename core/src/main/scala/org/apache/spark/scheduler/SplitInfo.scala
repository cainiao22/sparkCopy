package org.apache.spark.scheduler

// information about a specific split instance : handles both split instances.
// So that we do not need to worry about the differences.
class SplitInfo(
               val inputFormatClazz:Class[_],
               val hostLocation:String,
               val path:String,
               val length:Long,
               val underlyingSplit:Any
                 ) {

  override def toString(): String = {
    "SplitInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz +
      ", hostLocation : " + hostLocation + ", path : " + path +
      ", length : " + length + ", underlyingSplit " + underlyingSplit
  }

  override def hashCode:Int = {
    var hashcode = inputFormatClazz.hashCode()
    hashcode = hashcode * 31 + hostLocation.hashCode
    hashcode = hashcode * 31 + path.hashCode
    // ignore overflow ? It is hashcode anyway !
    hashcode = hashcode * 31 + (length & 0x7fffffff).toInt
    hashcode
  }

}
