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
 * App/Stage shuffle info obj
 */
public class StageShuffleInfo {
    private final String appId;
    private final String appAttempt;
    private final int stageAttempt;
    private final int shuffleId;

    public StageShuffleInfo(String appId, String appAttempt, int stageAttempt, int shuffleId) {
        this.appId = appId;
        this.appAttempt = appAttempt;
        this.stageAttempt = stageAttempt;
        this.shuffleId = shuffleId;
    }

    public String getAppId() {
        return appId;
    }

    public String getAppAttempt() {
        return appAttempt;
    }

    public int getStageAttempt() {
        return stageAttempt;
    }

    public int getShuffleId() {
        return shuffleId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StageShuffleInfo that = (StageShuffleInfo) o;
        return stageAttempt == that.stageAttempt &&
                shuffleId == that.shuffleId &&
                Objects.equals(appId, that.appId) &&
                Objects.equals(appAttempt, that.appAttempt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(appId, appAttempt, stageAttempt, shuffleId);
    }

    @Override
    public String toString() {
        return "StageShuffleInfo{" +
                "appId='" + appId + '\'' +
                ", appAttempt='" + appAttempt + '\'' +
                ", stageAttempt=" + stageAttempt +
                ", shuffleId=" + shuffleId +
                '}';
    }
}
