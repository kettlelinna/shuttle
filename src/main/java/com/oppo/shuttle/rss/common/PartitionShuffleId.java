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

package com.oppo.shuttle.rss.common;


import java.util.Objects;

/**
 * App/Stage/Partition shuffle info obj
 */
public class PartitionShuffleId {
    private final StageShuffleInfo stageShuffleInfo;
    private final int partitionId;

    public PartitionShuffleId(StageShuffleInfo stageShuffleInfo, int partitionId) {
        this.stageShuffleInfo = stageShuffleInfo;
        this.partitionId = partitionId;
    }

    public PartitionShuffleId(String appId, String appAttempt, int stageAttempt, int shuffleId, int partitionId) {
        stageShuffleInfo = new StageShuffleInfo(appId, appAttempt, stageAttempt, shuffleId);
        this.partitionId = partitionId;
    }

    public StageShuffleInfo getStageShuffleInfo() {
        return stageShuffleInfo;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionShuffleId that = (PartitionShuffleId) o;
        return partitionId == that.partitionId &&
                Objects.equals(stageShuffleInfo, that.stageShuffleInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stageShuffleInfo, partitionId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionShuffleId{ stageShuffleInfo=").append(stageShuffleInfo.toString())
                .append(", partitionId=").append(partitionId).append("}");
        return sb.toString();
    }
}
