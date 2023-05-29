package it.unibo.aggrcompare

import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * 5th attempt: turn different concerns into different actors
 */
object Actors5 {
  type Nbr = String

  trait NeighborhoodManagementProtocol
  case class AddNeighbour(nbr: ActorRef[DeviceProtocol]) extends NeighborhoodManagementProtocol
  case class RemoveNeighbour(nbr: ActorRef[DeviceProtocol]) extends NeighborhoodManagementProtocol
  case class AddListener(nbr: ActorRef[NeighborhoodListenerProtocol]) extends NeighborhoodManagementProtocol

  trait NeighborhoodListenerProtocol
  case class Neighborhood(nbrs: Set[ActorRef[DeviceProtocol]]) extends NeighborhoodListenerProtocol with DeviceCommunicationProtocol

  trait LocalSensorProtocol
  trait NbrSensorProtocol

  case class Configuration(localSensors: Map[String,Behavior[LocalSensorProtocol]] = Map.empty,
                           nbrSensors: Map[String,Behavior[NbrSensorProtocol]] = Map.empty)

  def neighborhoodManager(nbrs: Set[ActorRef[DeviceProtocol]],
                          listeners: Set[ActorRef[NeighborhoodListenerProtocol]]): Behavior[NeighborhoodManagementProtocol] =
    Behaviors.receiveMessage {
      case AddNeighbour(nbr) =>
        val newNbrhood = nbrs + nbr
        listeners.foreach(_ ! Neighborhood(newNbrhood))
        neighborhoodManager(newNbrhood, listeners)
      case AddListener(l) =>
        l ! Neighborhood(nbrs)
        neighborhoodManager(nbrs, listeners + l)
      case RemoveNeighbour(nbr) =>
        val newNbrhood = nbrs - nbr
        listeners.foreach(_ ! Neighborhood(newNbrhood))
        neighborhoodManager(newNbrhood, listeners)
      // ...
    }

  trait Msg

  trait SchedulerProtocol
  case object ScheduleComputation extends SchedulerProtocol
  case class SetSchedulable(s: ActorRef[DeviceActorProtocol]) extends SchedulerProtocol
  case class SchedulerState(schedulable: Option[ActorRef[DeviceActorProtocol]],
                            computeSchedulingDelay: FiniteDuration = 1.second)

  def scheduler(schedulerState: SchedulerState): Behavior[SchedulerProtocol] = Behaviors.withTimers { timers =>
    Behaviors.receiveMessage {
      case SetSchedulable(s) =>
        scheduler(schedulerState.copy(schedulable = Some(s)))
      case ScheduleComputation =>
        schedulerState.schedulable.foreach(_ ! Round)
        timers.startSingleTimer(ScheduleComputation, schedulerState.computeSchedulingDelay)
        Behaviors.same
    }
  }

  trait DeviceManagerProtocol
  case object Start extends DeviceManagerProtocol
  case class DeviceComponents(id: String, deviceActor: ActorRef[DeviceActorProtocol], deviceComponents: Components) extends DeviceManagerProtocol

  trait DeviceActorProtocol
  case object Round extends DeviceActorProtocol
  case class GetComponents(replyTo: ActorRef[DeviceManagerProtocol]) extends DeviceActorProtocol with DeviceActorSetupProtocol

  case class Components(nbrManager: ActorRef[NeighborhoodManagementProtocol],
                        localSensors: Map[String,ActorRef[LocalSensorProtocol]],
                        nbrSensors: Map[String,ActorRef[NbrSensorProtocol]],
                        scheduler: ActorRef[SchedulerProtocol])

  trait DeviceActorSetupProtocol

  def deviceActorSetup(id: String,
                  config: Configuration = Configuration()): Behavior[DeviceActorSetupProtocol] = Behaviors.setup(ctx => {
    val nbrManager = ctx.spawn(neighborhoodManager(Set.empty, Set.empty), s"device_${id}_nbrManager")
    val localSensorActors = config.localSensors.map(s => s._1 -> ctx.spawn(s._2, s"device_${id}_sensor_${s._1}"))
    val nbrSensorActors = config.nbrSensors.map(s => s._1 -> ctx.spawn(s._2, s"device_${id}_nbrSensor_${s._1}"))
    val schedulerActor = ctx.spawn(scheduler(SchedulerState(None)), s"device_${id}_scheduler")
    val startingComponents = Components(nbrManager, localSensorActors, nbrSensorActors, schedulerActor)
    val devActor = ctx.spawn(deviceActor(id, config, startingComponents), s"device_$id")
    schedulerActor ! SetSchedulable(devActor)
    ctx.log.info(s"Setup of device ${id} done:\n${startingComponents}")
    Behaviors.receiveMessage {
      case GetComponents(replyTo) =>
        replyTo ! DeviceComponents(id, devActor, startingComponents)
        Behaviors.same
    }
  })

  def deviceActor(id: String,
                  config: Configuration = Configuration(),
                  components: Components): Behavior[DeviceActorProtocol] = Behaviors.receive { (ctx, msg) => msg match {
      case GetComponents(replyTo) =>
        replyTo ! DeviceComponents(id, ctx.self, components)
        Behaviors.same
      case Round => Behaviors.ignore
    } }

  case class SharedDeviceData(sharedSensorData: Map[String,Any], sharedProgramData: Map[String,Any])

  trait DeviceCommunicationProtocol
  case class ReceiveNbrMessage(from: String, data: SharedDeviceData) extends DeviceCommunicationProtocol
  case class SendNbrMessage(sender: String, data: SharedDeviceData) extends DeviceCommunicationProtocol
  case class CommNeighborhood(nbrs: Set[ActorRef[DeviceCommunicationProtocol]]) extends DeviceCommunicationProtocol

  def communication(nbrs: Set[ActorRef[DeviceCommunicationProtocol]]): Behavior[DeviceCommunicationProtocol] = Behaviors.receiveMessage {
    case CommNeighborhood(nbrs: Set[ActorRef[DeviceCommunicationProtocol]]) =>
      communication(nbrs)
    case Neighborhood(nbrs: Set[ActorRef[DeviceProtocol]]) =>
      ??? //communication(nbrs)
    case ReceiveNbrMessage(from, data) =>
      // should propagate data to components
      ???
    case SendNbrMessage(sender, data) =>
      nbrs.foreach(_ ! ReceiveNbrMessage(sender, data))
      Behaviors.same
  }

  /*
  trait ContextUpdateProtocol
  case class AcquireContext(replyTo: ActorRef[ContextBasedProtocol]) extends ContextUpdateProtocol

  trait RoundProtocol
  case object Tick extends RoundProtocol with ContextUpdateProtocol
  def round(contextUpdater: ActorRef[ContextUpdateProtocol]): Behavior[RoundProtocol] = Behaviors.receive { (ctx, msg) => msg match {
    case Tick =>
      val futureContext: Future[GenericContext] = contextUpdater ? AcquireContext(ctx.self)
  } }

  def contextUpdater[C](sensors: Map[String,Any], nbrSensors: Map[String,Map[ActorRef[ContextUpdateProtocol],Map[Nbr,Any]]]): Behavior[ContextUpdateProtocol] =
    Behaviors.receiveMessage {
      case AcquireContext(replyTo) =>
        replyTo ! GenericContext(sensors, nbrSensors)
        Behaviors.same
    }

  trait ContextBasedProtocol
  case class GenericContext(sensors: Map[String,Any], nbrSensors: Map[String,Map[ActorRef[ContextUpdateProtocol],Map[Nbr,Any]]]) extends ContextBasedProtocol

   */

  case class GradientContext (
                               val isSource: Boolean = false,
                               val neighboursGradients: Map[Nbr,Double] = Map.empty,
                               val neighboursDistances: Map[Nbr,Double] = Map.empty
                             )
  trait GradientProtocol
  case class ComputeGradient(c: GradientContext, replyTo: ActorRef[Double]) extends GradientProtocol

  def gradient(): Behavior[GradientProtocol] = Behaviors.receiveMessage {
    case ComputeGradient(c, replyTo) =>
      val g = if(c.isSource) 0.0 else {
        c.neighboursGradients.minByOption(_._2).map {
          case (nbr,nbrg) => nbrg + c.neighboursDistances(nbr)
        }.getOrElse(Double.PositiveInfinity)
      }
      replyTo ! g
      Behaviors.same
  }

  trait GradientContextProtocol
  case class SetSource(source: Boolean) extends GradientContextProtocol
  case class SetNeighbourGradient(nbr: Nbr, gradient: Double) extends GradientContextProtocol
  case class SetNeighbourDistance(nbr: Nbr, distance: Double) extends GradientContextProtocol
  case class Get(replyTo: ActorRef[GradientContext])

  def gradientContext(c: GradientContext): Behavior[GradientContextProtocol] = Behaviors.receiveMessage[GradientContextProtocol] {
    case SetSource(s) => gradientContext(c.copy(isSource = s))
    case SetNeighbourGradient(nbr, g) => gradientContext(c.copy(neighboursGradients = c.neighboursGradients + (nbr -> g)))
    case SetNeighbourDistance(nbr, d) => gradientContext(c.copy(neighboursDistances = c.neighboursDistances + (nbr -> d)))
    // TODO: must also consider that neighbours may be removed together their data
  }

  trait ChannelContext {
    val distanceToSource: Double
    val distanceToTarget: Double
    val distanceBetweenSourceAndTarget: Double
    val tolerance: Double
  }
  trait ChannelProtocol
  case class ComputeChannel(c: ChannelContext, replyTo: ActorRef[Boolean]) extends ChannelProtocol

  def channel(): Behavior[ChannelProtocol] = Behaviors.receiveMessage {
    case ComputeChannel(c, replyTo) =>
      val channel = c.distanceToSource + c.distanceToTarget <= c.distanceBetweenSourceAndTarget + c.tolerance
      replyTo ! channel
      Behaviors.same
  }
}


object Actors5App extends App {
  println("Actors implementation")
  import Actors5._

  var map = Map[Int, ActorRef[DeviceActorSetupProtocol]]()
  var componentsMap = Map[Int, DeviceComponents]()
  val system = ActorSystem[DeviceManagerProtocol](Behaviors.setup { ctx =>
    // 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 10   (IDs)
    // --------------------------------------
    // 2 - 1 - 0 - 1 - 2 - 3 - 4 - 5 - 6 - 7    (gradient)
    for(i <- 1 to 10) {
      map += i -> ctx.spawn(deviceActorSetup(s"d_$i"), s"device_actor_$i")
      implicit val timeout: Timeout = 2.seconds
      ctx.ask[GetComponents, DeviceComponents](map(i), (ref: ActorRef[_]) => GetComponents(ctx.self)) {
        case scala.util.Success(dc @ DeviceComponents(_, d, c)) =>
          componentsMap += i -> dc
          dc
        case _ => ???
      }
    }
    Behaviors.ignore
  }, "ActorBasedChannel")
}