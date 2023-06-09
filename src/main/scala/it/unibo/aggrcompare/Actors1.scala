package it.unibo.aggrcompare

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import it.unibo.scafi.space.Point3D

import scala.concurrent.duration.DurationInt

object SystemMessages {
  case object Start
}

/**
 * 1st attempt: an actor computing a gradient, atomic behaviour, no separation of concerns
 */
object Actors1 {
  val RETENTION_TIME = 10000 // 10 seconds
  def currentTime(): Long = System.currentTimeMillis()

  sealed trait Msg
  case class SetSource(isSource: Boolean) extends Msg
  case class SetPosition(point: Point3D) extends Msg
  case class GetPosition(replyTo: ActorRef[NbrPos]) extends Msg
  case class SetDistance(distance: Double, from: ActorRef[Msg]) extends Msg
  case object ComputeGradient extends Msg
  case class QueryGradient(replyTo: ActorRef[NbrGradient]) extends Msg
  case class SetNeighbourGradient(distance: Double, from: ActorRef[Msg]) extends Msg
  case class AddNeighbour(nbr: ActorRef[Msg]) extends Msg
  case class RemoveNeighbour(nbr: ActorRef[Msg]) extends Msg
  case object Round extends Msg
  case object Stop extends Msg

  case class NbrPos(position: Point3D, nbr: ActorRef[Msg])
  case class NbrGradient(gradient: Double, nbr: ActorRef[Msg])

  def apply(source: Boolean,
            gradient: Double,
            nbrs: Map[ActorRef[Msg],Long],
            distances: Map[ActorRef[Msg],Double],
            nbrGradients: Map[ActorRef[Msg],Double],
            position: Point3D = Point3D(0,0,0)): Behavior[Msg] = Behaviors.setup{ ctx =>
    val getPositionAdapter: ActorRef[NbrPos] = ctx.messageAdapter(m => SetDistance(m.position.distance(position), m.nbr))
    val getGradientAdapter: ActorRef[NbrGradient] = ctx.messageAdapter(m => SetNeighbourGradient(m.gradient, m.nbr))

    Behaviors.withTimers { timers => Behaviors.receive { case (ctx,msg) =>
    msg match {
      case SetSource(s) =>
        Actors1(s, 0, nbrs, distances, nbrGradients, position)
      case AddNeighbour(nbr) =>
        Actors1(source, gradient, nbrs + (nbr -> System.currentTimeMillis()), distances, nbrGradients, position)
      case RemoveNeighbour(nbr) =>
        Actors1(source, gradient, nbrs - nbr, distances, nbrGradients, position)
      case SetPosition(p) =>
        Actors1(source, gradient, nbrs, distances, nbrGradients, p)
      case GetPosition(replyTo) =>
        replyTo ! NbrPos(position, ctx.self)
        Behaviors.same
      case SetDistance(d, from) =>
        Actors1(source, gradient, nbrs + (from -> currentTime()), distances + (from -> d), nbrGradients, position)
      case ComputeGradient => {
        val newNbrGradients = nbrGradients + (ctx.self -> gradient)
        val disalignedNbrs = nbrs.filter(nbr => currentTime() - nbrs.getOrElse(nbr._1, Long.MinValue) > RETENTION_TIME).keySet
        val alignedNbrGradients = newNbrGradients -- disalignedNbrs
        val alignedDistances = distances -- disalignedNbrs

        // Once gradient is computed, start the next round in a second
        timers.startSingleTimer(Round, 1.second)

        // ctx.log.info(s"${ctx.self.path.name} CONTEXT\nDisaligned nbrs: ${disalignedNbrs}\nNbrs: ${nbrs}\nDistances: ${distances}\nNbrGradients:${nbrGradients}")

        if(source){
          ctx.log.info(s"GRADIENT (SOURCE): ${ctx.self.path.name} -> ${gradient}")
          Actors1(source, 0, nbrs, distances, newNbrGradients, position)
        } else {
          val updatedG = (alignedNbrGradients - ctx.self).map(n => n -> (n._2
            + alignedDistances.get(n._1).getOrElse(Double.PositiveInfinity))).values.minOption
            .getOrElse(Double.PositiveInfinity)
          ctx.log.info(s"GRADIENT: ${ctx.self.path.name} -> ${updatedG}")
          Actors1(source, updatedG, nbrs, distances, nbrGradients + (ctx.self -> updatedG), position)
        }
      }
      case QueryGradient(replyTo) => {
        replyTo ! NbrGradient(gradient, ctx.self)
        Behaviors.same
      }
      case SetNeighbourGradient(d, from) =>
        Actors1(source, gradient, nbrs + (from -> currentTime()), distances, nbrGradients + (from -> d), position)
      case Round => {
        nbrs.keySet.foreach(nbr => {
          // Query neighbour for neighbouring sensors
          nbr ! GetPosition(getPositionAdapter)
          // Query neighbour for application data
          nbr ! QueryGradient(getGradientAdapter)
        })
        timers.startSingleTimer(ComputeGradient, 1.seconds)
        Behaviors.same
      }
      case Stop => Behaviors.stopped
    }
  } } }
}


object Actors1App extends App {
  println("Actors implementation")

  var map = Map[Int, ActorRef[Actors1.Msg]]()
  val system = ActorSystem[SystemMessages.Start.type](Behaviors.setup { ctx =>
    // 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 10   (IDs)
    // --------------------------------------
    // 2 - 1 - 0 - 1 - 2 - 3 - 4 - 5 - 6 - 7    (gradient)
    for(i <- 1 to 10) {
      map += i -> ctx.spawn(Actors1(false, Double.PositiveInfinity, Map.empty, Map.empty, Map.empty), s"device-${i}")
    }
    map.keys.foreach(d => {
      map(d) ! Actors1.SetPosition(Point3D(d,0,0))
      if(d>1) map(d) ! Actors1.AddNeighbour(map(d - 1))
      if(d<10) map(d) ! Actors1.AddNeighbour(map(d + 1))
    })

    map(3) ! Actors1.SetSource(true)

    map.values.foreach(_ ! Actors1.Round)

    Behaviors.ignore
  }, "ActorBasedChannel")

  Thread.sleep(10000)

  map(4) ! Actors1.Stop
}