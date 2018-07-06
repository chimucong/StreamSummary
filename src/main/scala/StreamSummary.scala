import scala.collection.mutable

class Bucket[T](var count: Long) {
  var prev: Bucket[T] = _
  var next: Bucket[T] = _
  val set: mutable.HashSet[T] = mutable.HashSet.empty
  var list: DoubleLinkedList[T] = _

  def size = set.size

  def remove(item: T): Unit = {
    set -= item
  }

  def add(item: T): Unit = {
    set += item
  }

  def insertBeforeSelf(other: Bucket[T]): Unit = {
    require(list != null)
    other.list = list
    if (prev == null) {
      list.head = other
      other.prev = null
      other.next = this
      prev = other
    } else {
      prev.next = other
      other.prev = prev
      other.next = this
      prev = other
    }
  }

  def removeFromList(): Unit = {
    require(list != null)
    if (list.head == this) {
      list.head = this.next
    }
    if (list.tail == this) {
      list.tail = this.prev
    }
    if (prev != null) {
      prev.next = next
    }
    if (next != null) {
      next.prev = prev
    }
  }

  override def toString: String = s"Bucket[count: $count]"
}

class BucketItem[T](private var item: T, var count: Long, var error: Long) {
  var bucket: Bucket[T] = _

  def getItem = item

  def setItem(other: T): Unit = {
    require(bucket != null)
    bucket.set -= item
    bucket.set += other
    item = other
  }

  def increase(): Unit = {
    require(bucket != null)

    if (bucket.size == 1) {
      if (bucket.prev == null || bucket.prev.count > count + 1) {
        bucket.count += 1
      } else {
        bucket.removeFromList()
        bucket.prev.add(item)
        bucket = bucket.prev
      }
    } else {
      if (bucket.prev == null || bucket.prev.count > count) {
        val newBucket = new Bucket[T](count + 1)
        bucket.insertBeforeSelf(newBucket)
      }
      bucket.remove(item)
      bucket.prev.add(item)
      bucket = bucket.prev
    }
    count += 1
  }

  override def toString: String = s"[item: $item, count: $count, error: $error]"
}

class DoubleLinkedList[T]() {
  var head: Bucket[T] = _
  var tail: Bucket[T] = _

  def min: T = {
    require(tail != null)
    tail.set.head
  }


  def add(item: BucketItem[T]): Unit = {
    require(item.count == 1)
    if (tail == null || tail.count > 1) {
      val bucket = new Bucket[T](1)
      addToTail(bucket)
      bucket.add(item.getItem)
      item.bucket = bucket
    } else {
      tail.add(item.getItem)
      item.bucket = tail
    }
  }

  private def addToTail(bucket: Bucket[T]): Unit = {
    bucket.list = this
    if (tail == null) {
      head = bucket
      tail = bucket
      bucket.prev = null
      bucket.next = null
    } else {
      tail.next = bucket
      bucket.prev = tail
      tail = bucket
      bucket.next = null
    }
  }
}


class StreamSummary[T](capacity: Int) {
  var size: Int = 0
  val counters = new mutable.HashMap[T, BucketItem[T]]()
  val list = new DoubleLinkedList[T]

  private def introduce(item: T): Unit = {
    if (size < capacity) {
      size += 1
      val bucketItem = new BucketItem[T](item, 1, 0)
      list.add(bucketItem)
      counters.put(item, bucketItem)
    } else {
      val Some(minItem) = counters.get(list.min)
      val count = minItem.count
      minItem.increase()
      minItem.error = count
      counters -= minItem.getItem
      minItem.setItem(item)
      counters.put(item, minItem)
    }
  }

  def add(item: T): Unit = {
    counters.get(item) match {
      case None => introduce(item)
      case Some(bucketItem) => bucketItem.increase()
    }
  }

  def topK(k: Int): Seq[(Long, Long, T)] = {
    require(k <= capacity)
    val array = new Array[(Long, Long, T)](k)
    var bucket = list.head
    var count = 0
    while (count < k) {
      val set = if (bucket.set.size <= k - count) {
        bucket.set
      } else {
        bucket.set.take(k - count)
      }
      set.foreach(item => {
        val Some(bucketItem) = counters.get(item)
        array(count) = (bucketItem.count, bucketItem.error, bucketItem.getItem)
        count += 1
      })
      bucket = bucket.next
    }
    return array
  }
}
