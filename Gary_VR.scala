package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import scala.io.Source
import scala.math.sqrt

object Gary_VR {

  def village_name() : Map[Int ,String] = {

    /*implicit val codec  : Codec = Codec("UTF=-8")

    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE) */

    var map_vill : Map[Int,String] = Map()

    val lines = Source.fromFile("data/village_name_and_ID - Sheet1.csv")

    for(l <- lines.getLines()) {
      val fld = l.split(',')
      if (fld.length > 1) {
        map_vill += (fld(0).toInt -> fld(1))
      }
    }
    lines.close()

    map_vill
  }
  type q1 = (Int,Double)
  type q2 = (Int,(q1,q1))
  def pair(ur : q2) : ((Int,Int),(Double,Double)) = {

    val vr_1 = ur._2._1
    val vr_2 = ur._2._2

    val v1 = vr_1._1
    val r1  = vr_1._2
    val v2 = vr_2._1
    val r2 = vr_2._2


    ((v1,v2),(r1,r2))

    }

  def redun(ur : q2) : Boolean = {

    val vr_1 = ur._2._1
    val vr_2 = ur._2._2

    val v1 = vr_1._1
    val v2 = vr_2._1

    v1 < v2
  }

  type rp = (Double,Double)
  type rps = Iterable[rp]

  def cosine(k : rps) : (Double,Int) = {
    var np : Int = 0
    var aa: Double = 0.0
    var ab: Double = 0.0
    var bb: Double = 0.0

    for (p <- k) {
      val ra = p._1
      val rb = p._2

      aa += ra * ra
      ab += ra * rb
      bb += rb * rb

      np +=1
    }

    val nmt : Double = ab
    val dmt : Double = sqrt(aa) * sqrt(bb)

    var scr : Double = 0.0

    if(dmt !=0) {
      scr = nmt / dmt
    }

    (scr , np)
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Gary_VR")

    val dct = village_name()

    val dt = sc.textFile("data/vr7.data")

    val rt = dt.map(ln => ln.split(",")).map(ln => (ln(0).toInt,(ln(1).toInt , ln(2).toDouble)))

    val jrt = rt.join(rt)

    val redun_jrt = jrt.filter(redun)

    val  vp = redun_jrt.map(pair)

    val  vpr = vp.groupByKey()

    val  vpr_rmd = vpr.mapValues(cosine).cache()

      val r_thresh = 0.6
      val occ_thresh = 2

      val id : Int = 6

    val flt_rst = vpr_rmd.filter( d =>
    {
      val t = d._1
      val y = d._2
      (t._1 == id || t._2 == id) && y._1 > r_thresh && y._2 > occ_thresh
    }
    )

      val rst = flt_rst.map(x => (x._2,x._1)).sortByKey(ascending = false).take(5)

      println("\nTop 5 villages recommended  with respect to " + dct(id) + "\n")
      for(r <- rst){
        val sim = r._1
        val pair  = r._2
        var sim_vil_id = pair._1

        if(sim_vil_id == id) {
          sim_vil_id = pair._2
        }
        println("\t" + "->" + dct(sim_vil_id))
      }



  }

}
