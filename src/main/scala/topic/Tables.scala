/**
  * Created by shawn on 2/20/17.
  */

package topic

import slick.driver.H2Driver.api._
import slick.lifted.{ProvenShape, ForeignKeyQuery}

/**
  * Created by shawn on 2/15/17.
  */
class Pubmeds(tag: Tag)
  extends Table[(Int, Int, String, String, String, String, Int)](tag, "PUBMEDS") {
  def id: Rep[Int] = column[Int]("ID", O.PrimaryKey, O.AutoInc)

  def pmid: Rep[Int] = column[Int]("PMID")

  def matched: Rep[String] = column[String]("MATCHED")

  def concept: Rep[String] = column[String]("CONCEPT")

  def preferred: Rep[String] = column[String]("PREFERRED")

  def meshId: Rep[String] = column[String]("MESH_ID")

  def isTopic: Rep[Int] = column[Int]("IS_TOPIC")

  def * : ProvenShape[(Int, Int, String, String, String, String, Int)] =
    (id, pmid, matched, concept, preferred, meshId, isTopic)
}
