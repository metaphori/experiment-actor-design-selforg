package it.unibo.aggrcompare

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import com.typesafe.config.ConfigFactory
import it.unibo.aggrcompare.Actors1App.map
import it.unibo.scafi.space.Point3D

import scala.concurrent.duration.DurationInt
import scala.util.Random

/**
 * 3rd attempt: OOP style
 */

object Actors3 {
  trait DeviceProtocol
  object DeviceProtocol {
    case class SetSensor[T](name: String, value: T) extends DeviceProtocol
    case class AddToMapSensor[T](name: String, value: T) extends DeviceProtocol
    case class RemoveToMapSensor[T](name: String, value: T) extends DeviceProtocol
    case class SetNbrSensor[T](name: String, nbr: ActorRef[DeviceProtocol], value: T) extends DeviceProtocol
    case class Compute(what: String) extends DeviceProtocol
    case class AddNeighbour(nbr: ActorRef[DeviceProtocol]) extends DeviceProtocol
    case class RemoveNeighbour(nbr: ActorRef[DeviceProtocol]) extends DeviceProtocol
    case object Stop extends DeviceProtocol
  }

  object Sensors {
    val nbrRange = "nbrRange"
    val source = "source"
    val position = "position"
    val neighbors = "nbrs"
  }

  object Data {
    val gradient = "gradient"
  }

  val RETENTION_TIME = 10000 // 10 seconds
  def currentTime(): Long = System.currentTimeMillis()

  abstract class DeviceActor[T](context: ActorContext[DeviceProtocol]) extends AbstractBehavior(context) {

    import DeviceProtocol._

    var localSensors = Map[String, Any]()
    type Nbr = ActorRef[DeviceProtocol]

    def sense[T](name: String): T = localSensors(name).asInstanceOf[T]
    def senseOrElse[T](name: String, default: => T): T = localSensors.getOrElse(name, default).asInstanceOf[T]
    def nbrSense[T](name: String)(id: Nbr): Option[T] = nbrValue(name).get(id).filter((v: T) => neighbors.contains(id))
    def nbrValue[T](name: String): Map[Nbr, T] = senseOrElse[Map[Nbr, T]](name, Map.empty).filter(tp => neighbors.contains(tp._1))
    def neighbors: Set[ActorRef[DeviceProtocol]] = senseOrElse[Map[Nbr, Long]](Sensors.neighbors, Map.empty)
      .filter(tp => currentTime() - tp._2 < RETENTION_TIME).keySet
    def updateNbrTimestamp(nbr: Nbr, t: Long = currentTime()): Unit =
      localSensors += Sensors.neighbors -> (nbrValue[Long](Sensors.neighbors) + (nbr -> t))

    def name: String = context.self.path.name
    def compute(what: String, d: DeviceActor[T]): T

    override def onMessage(msg: DeviceProtocol): Behavior[DeviceProtocol] = Behaviors.withTimers { timers =>
      msg match { // NB: no Behaviors.receiveMessage since the current message is the parameter of onMessage!!!
        case SetSensor(sensorName, value) =>
          // context.log.info(s"${this.name}: setting sensor $sensorName  to $value ")
          localSensors += (sensorName -> value)
          this
          /*
        case AddToMapSensor(name, value: (Nbr,_)) =>
          // context.log.info(s"${this.name}: adding to mapsensor $name  value $value ")
          localSensors += name -> (localSensors.getOrElse(name, Map.empty).asInstanceOf[Map[Any,Any]] + value)
          updateNbrTimestamp(value._1)
          this
        case RemoveToMapSensor(name, value: (Nbr,_)) =>
          // context.log.info(s"${this.name}: removing from mapsensor $name value $value (its key) ")
          localSensors += name -> (localSensors.getOrElse(name, Map.empty).asInstanceOf[Map[Any,Any]] - value._1)
          updateNbrTimestamp(value._1, 0) // localSensors += Sensors.neighbors -> (nbrValue[Long](Sensors.neighbors) + (value._1.asInstanceOf[Nbr] -> 0))
          this
           */
        case SetNbrSensor(name, nbr, value) =>
          // context.log.info(s"${this.name}: setting nbrsensor $name  to $nbr -> $value ")
          val sval = localSensors.getOrElse(name, Map.empty).asInstanceOf[Map[Nbr, Any]]
          localSensors += name -> (sval + (nbr -> value))
          updateNbrTimestamp(nbr) // localSensors += Sensors.neighbors -> (nbrValue[Long](Sensors.neighbors) + (nbr -> currentTime()))
          this
        case Compute(what) =>
          val result = compute(what, this)
          context.self ! SetNbrSensor(what, context.self, result)
          neighbors.foreach(_ ! SetNbrSensor(what, context.self, result))
          timers.startSingleTimer(Compute(what), 2.seconds * (1+Random.nextInt(2)))
          context.log.info(s"${name} computes: ${result}")
          this
        case AddNeighbour(nbr) =>
          context.log.info(s"${this.name}: adding neighbour $nbr")
          context.self ! SetNbrSensor(Sensors.neighbors, nbr, currentTime())
          context.self ! SetNbrSensor(Sensors.nbrRange, nbr, 1.0)
          this
        case RemoveNeighbour(nbr) =>
          context.log.info(s"${this.name}: removing neighbour $nbr")
          context.self ! SetNbrSensor(Sensors.neighbors, nbr, 0)
          this
        case Stop =>
          Behaviors.stopped
      }
    }
  }

  object DeviceActor {
    def apply[T](c: (ActorContext[DeviceProtocol], String, DeviceActor[T]) => T): Behavior[DeviceProtocol] =
      Behaviors.setup(ctx => new DeviceActor[T](ctx) {
        ctx.log.info(s"INSTANTIATING ACTOR ${ctx.self.path}")
        override def compute(what: String, deviceActor: DeviceActor[T]): T = c(ctx, what, deviceActor)
      })
  }
}

object Actors3App extends App {
  import Actors3._
  import DeviceProtocol._
  println("Actors OOP-style implementation")

  var map = Map[Int, ActorRef[DeviceProtocol]]()
  val system = ActorSystem[SystemMessages.Start.type](Behaviors.setup { ctx =>
    for(i <- 1 to 10) {
      map += i -> ctx.spawn(DeviceActor[Double]((ctx,w,d) => {
        // ctx.log.info(s"${d.name}'s context: ${d.localSensors}\nALIGNED NEIGHBORS: ${d.neighbors}")
        val nbrg = d.nbrValue[Double](Data.gradient)
          .map(n => n._2 + d.nbrSense[Double](Sensors.nbrRange)(n._1).getOrElse(Double.PositiveInfinity))
          .minOption.getOrElse(Double.PositiveInfinity)
        if(d.senseOrElse("source", false)) 0.0 else nbrg
      }),s"device-${i}")
    }

    Behaviors.receiveMessage {
      case SystemMessages.Start =>
        map.keySet.foreach(d => {
          map(d) ! SetSensor(Sensors.position, Point3D(d,0,0))
          map(d) ! SetSensor(Sensors.source, false)
          if(d > 1) map(d) ! AddNeighbour(map(d - 1))
          if(d < 10) map(d) ! AddNeighbour(map(d + 1))
        })

        map(3) ! SetSensor(Sensors.source, true)

        map.values.foreach(_ ! Compute(Data.gradient))
        Behaviors.ignore
    }
  }, "ActorBasedGradient", ConfigFactory.defaultReference())

  system ! SystemMessages.Start

  Thread.sleep(10000)

  map(4) ! Stop
}