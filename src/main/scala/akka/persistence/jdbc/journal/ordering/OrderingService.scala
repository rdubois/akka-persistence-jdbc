package akka.persistence.jdbc.journal.ordering

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.persistence.jdbc.journal.ordering.OrderingActor.BatchResult
import akka.util.Timeout

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

object OrderingService {
  def apply(orderingActor: ActorRef)(implicit ec: ExecutionContext, timeout: Timeout): OrderingService =
    new DefaultOrderingService(orderingActor)
}

trait OrderingService {
  /**
   * Initializes the counter
   */
  def initialize(maxOrdering: Long): Future[Done]

  /**
   * returns a list of numbers
   * @param size
   */
  def getBatch(size: Long): Future[BatchResult]
}

private[ordering] class DefaultOrderingService(orderingActor: ActorRef)(implicit ec: ExecutionContext, timeout: Timeout) extends OrderingService {
  override def initialize(maxOrdering: Long): Future[Done] = for {
    done <- (orderingActor ? OrderingActor.Init(maxOrdering)).mapTo[Done]
  } yield done

  override def getBatch(size: Long): Future[BatchResult] = for {
    batch <- (orderingActor ? OrderingActor.GetBatch(size)).mapTo[BatchResult]
  } yield batch
}

object OrderingActor {
  final case class Init(highestNumber: Long)
  final case class GetBatch(size: Long)
  final case class BatchResult(xs: Seq[Long])
  def props(): Props = Props[OrderingActor]
}

private[ordering] class OrderingActor extends Actor {
  override def preStart(): Unit = {
    context.become(initialized(0L))
  }

  override def receive: Receive = PartialFunction.empty

  def initialized(num: Long): Receive = LoggingReceive {
    case OrderingActor.Init(highest) =>
      context.become(initialized(highest))
      sender() ! Done

    case OrderingActor.GetBatch(size) =>
      val xs: Seq[Long] = (1L to size).map(i => (_: Long) + i).map(_.apply(num))
      val batchIncrementedNumber = num + size
      context.become(initialized(batchIncrementedNumber))
      sender() ! BatchResult(xs)
  }
}