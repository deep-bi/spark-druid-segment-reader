package bi.deep.segments

object MixedGranularitySolver {
  trait Overlapping[T] {

    /** Defines if two values overlaps (have common part) */
    def overlaps(left: T, right: T): Boolean

  }

  private def upsert[K, V](map: Map[K, V])(key: K, value: V)(f: (V, V) => V): Map[K, V] = {
    map.get(key) match {
      case None => map.updated(key, value)
      case Some(existing) => map.updated(key, f(existing, value))
    }
  }

  private def upsertMany[K, V](map: Map[K, V])(kv: (K, V)*)(f: (V, V) => V): Map[K, V] = {
    kv.foldLeft(map) { case (map, (k, v)) => upsert(map)(k, v)(f) }
  }

  def solve[T](values: List[T])(implicit overlapping: Overlapping[T], ordering: Ordering[T]): List[T] = {
    if (values.length < 2) values
    else {
      val collisions = values.combinations(2).foldLeft(Map.empty[T, Set[T]]) { case (collisions, left :: right :: Nil) =>
        if (overlapping.overlaps(left, right)) upsertMany(collisions)(
          left -> Set(left, right),
          right -> Set(right, left)
        )(_ ++ _)
        else upsertMany(collisions)(
          left -> Set(left),
          right -> Set(right)
        )(_ ++ _)
      }

      val results = collisions.foldLeft(Set.empty[T]) { case (solutions, (_, collisions)) => solutions + collisions.max }
      results.toList
    }

  }
}
