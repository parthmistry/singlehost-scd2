package post.parthmistry.singlehostscd2.service

import java.lang

sealed trait ProductIdGenerator {

  def generateId(id: lang.Long): String

}

class BasicIdGenerator extends ProductIdGenerator {

  override def generateId(id: lang.Long): String = {
    "PID" + id
  }

}

class LongIdGenerator extends ProductIdGenerator {

  override def generateId(id: lang.Long): String = {
    String.format("PROD-%010d-0%d", id, lang.Long.valueOf(id % 10))
  }

}
