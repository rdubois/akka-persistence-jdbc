/*
 * Copyright 2016 Dennis Vriend
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package akka.persistence.jdbc.journal

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem, Stash}
import akka.event.LoggingReceive
import akka.pattern.pipe
import akka.persistence.jdbc.config.JournalConfig
import akka.persistence.jdbc.journal.JdbcAsyncWriteJournal.SetHighestOrdering
import akka.persistence.jdbc.journal.dao.JournalDao
import akka.persistence.jdbc.journal.ordering.{OrderingActor, OrderingService}
import akka.persistence.jdbc.util.{SlickDatabase, SlickDriver}
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import slick.jdbc.JdbcProfile
import slick.jdbc.JdbcBackend._

import scala.collection.immutable._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object JdbcAsyncWriteJournal {
  case object Init
  case class InitDone(highestOrdering: Long)
  case class SetHighestOrdering(highestOrdering: Long)
}

class JdbcAsyncWriteJournal(config: Config) extends AsyncWriteJournal with Stash {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  val journalConfig = new JournalConfig(config)
  val orderingActor: ActorRef = system.actorOf(OrderingActor.props())
  val orderingService: OrderingService = OrderingService(orderingActor)(ec, journalConfig.pluginConfig.orderingTimeout)

  val db: Database = SlickDatabase.forConfig(config, journalConfig.slickConfiguration)

  val journalDao: JournalDao = {
    val fqcn = journalConfig.pluginConfig.dao
    val profile: JdbcProfile = SlickDriver.forDriverName(config)
    val args = Seq(
      (classOf[Database], db),
      (classOf[JdbcProfile], profile),
      (classOf[JournalConfig], journalConfig),
      (classOf[Serialization], SerializationExtension(system)),
      (classOf[OrderingService], orderingService),
      (classOf[ExecutionContext], ec),
      (classOf[Materializer], mat)
    )
    system.asInstanceOf[ExtendedActorSystem].dynamicAccess.createInstanceFor[JournalDao](fqcn, args) match {
      case Success(dao)   => dao
      case Failure(cause) => throw cause
    }
  }

  override def preStart(): Unit = {
    context.become(initializing)
    self ! JdbcAsyncWriteJournal.Init
  }

  def initialize(): Future[JdbcAsyncWriteJournal.InitDone] = for {
    highestOrdering <- journalDao.highestOrdering()
    _ <- orderingService.initialize(highestOrdering)
  } yield JdbcAsyncWriteJournal.InitDone(highestOrdering)

  def initializing: Receive = {
    case JdbcAsyncWriteJournal.Init =>
      initialize().pipeTo(self)
    case akka.actor.Status.Failure(t) =>
      throw t
    case JdbcAsyncWriteJournal.InitDone(highestOrdering) =>
      context.become(super.receive)
      unstashAll()
    case msg =>
      stash()
  }

  override def receivePluginInternal: Receive = LoggingReceive {
    case SetHighestOrdering(highestOrdering) =>
      orderingService.initialize(highestOrdering).pipeTo(sender())
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // get highest sequence nr for initialization
    // enrich messages with highest sequence nr
    Source(messages)
      .via(journalDao.writeFlow)
      .runFold(List.empty[Try[Unit]])(_ :+ _)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    journalDao.delete(persistenceId, toSequenceNr)

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    journalDao.highestSequenceNr(persistenceId, fromSequenceNr)

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: (PersistentRepr) => Unit): Future[Unit] =
    journalDao.messages(persistenceId, fromSequenceNr, toSequenceNr, max)
      .mapAsync(1)(deserializedRepr => Future.fromTry(deserializedRepr))
      .runForeach(recoveryCallback)
      .map(_ => ())

  override def postStop(): Unit = {
    db.close()
    super.postStop()
  }
}
