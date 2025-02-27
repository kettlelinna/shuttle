/*
 * Copyright 2021 OPPO. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle
import com.oppo.shuttle.rss.exceptions.Ors2Exception
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.storage.{BlockId, ShuffleMergedBlockId}

class Ors2ShuffleBlockResolver extends ShuffleBlockResolver {
  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    throw new Ors2Exception("Ors2 remote shuffle no need to implement ShuffleBlockResolver.")
  }

  override def stop(): Unit = {

  }

  override def getMergedBlockData(blockId: ShuffleMergedBlockId, dirs: Option[Array[String]]): Seq[ManagedBuffer] = ???

  override def getMergedBlockMeta(blockId: ShuffleMergedBlockId, dirs: Option[Array[String]]): MergedBlockMeta = ???
}
