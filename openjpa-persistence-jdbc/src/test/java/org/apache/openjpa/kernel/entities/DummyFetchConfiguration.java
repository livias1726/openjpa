/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.openjpa.kernel.entities;

import org.apache.openjpa.kernel.DataCacheRetrieveMode;
import org.apache.openjpa.kernel.DataCacheStoreMode;
import org.apache.openjpa.kernel.FetchConfiguration;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.lib.rop.ResultList;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.FieldMetaData;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DummyFetchConfiguration implements FetchConfiguration {
    @Override
    public StoreContext getContext() {
        return null;
    }

    @Override
    public void setContext(StoreContext ctx) {

    }

    @Override
    public Object clone() {
        return null;
    }

    @Override
    public void copy(FetchConfiguration fetch) {

    }

    @Override
    public int getFetchBatchSize() {
        return 0;
    }

    @Override
    public FetchConfiguration setFetchBatchSize(int fetchBatchSize) {
        return null;
    }

    @Override
    public int getMaxFetchDepth() {
        return 0;
    }

    @Override
    public FetchConfiguration setMaxFetchDepth(int max) {
        return null;
    }

    @Override
    public boolean getQueryCacheEnabled() {
        return false;
    }

    @Override
    public FetchConfiguration setQueryCacheEnabled(boolean cache) {
        return null;
    }

    @Override
    public int getFlushBeforeQueries() {
        return 0;
    }

    @Override
    public FetchConfiguration setFlushBeforeQueries(int flush) {
        return null;
    }

    @Override
    public boolean getExtendedPathLookup() {
        return false;
    }

    @Override
    public FetchConfiguration setExtendedPathLookup(boolean flag) {
        return null;
    }

    @Override
    public Set<String> getFetchGroups() {
        return null;
    }

    @Override
    public boolean hasFetchGroup(String group) {
        return false;
    }

    @Override
    public FetchConfiguration addFetchGroup(String group) {
        return null;
    }

    @Override
    public FetchConfiguration addFetchGroups(Collection<String> groups) {
        return null;
    }

    @Override
    public FetchConfiguration removeFetchGroup(String group) {
        return null;
    }

    @Override
    public FetchConfiguration removeFetchGroups(Collection<String> groups) {
        return null;
    }

    @Override
    public FetchConfiguration clearFetchGroups() {
        return null;
    }

    @Override
    public FetchConfiguration resetFetchGroups() {
        return null;
    }

    @Override
    public Set<String> getFields() {
        return null;
    }

    @Override
    public boolean hasField(String field) {
        return false;
    }

    @Override
    public FetchConfiguration addField(String field) {
        return null;
    }

    @Override
    public FetchConfiguration addFields(Collection<String> fields) {
        return null;
    }

    @Override
    public FetchConfiguration removeField(String field) {
        return null;
    }

    @Override
    public FetchConfiguration removeFields(Collection<String> fields) {
        return null;
    }

    @Override
    public FetchConfiguration clearFields() {
        return null;
    }

    @Override
    public int getLockTimeout() {
        return 0;
    }

    @Override
    public FetchConfiguration setLockTimeout(int timeout) {
        return null;
    }

    @Override
    public int getLockScope() {
        return 0;
    }

    @Override
    public FetchConfiguration setLockScope(int scope) {
        return null;
    }

    @Override
    public int getQueryTimeout() {
        return 0;
    }

    @Override
    public FetchConfiguration setQueryTimeout(int timeout) {
        return null;
    }

    @Override
    public int getReadLockLevel() {
        return 0;
    }

    @Override
    public FetchConfiguration setReadLockLevel(int level) {
        return null;
    }

    @Override
    public int getWriteLockLevel() {
        return 0;
    }

    @Override
    public DataCacheStoreMode getCacheStoreMode() {
        return null;
    }

    @Override
    public void setCacheStoreMode(DataCacheStoreMode mode) {

    }

    @Override
    public DataCacheRetrieveMode getCacheRetrieveMode() {
        return null;
    }

    @Override
    public void setCacheRetrieveMode(DataCacheRetrieveMode mode) {

    }

    @Override
    public FetchConfiguration setWriteLockLevel(int level) {
        return null;
    }

    @Override
    public ResultList<?> newResultList(ResultObjectProvider rop) {
        return null;
    }

    @Override
    public void setHint(String name, Object value, Object original) {

    }

    @Override
    public void setHint(String key, Object value) {

    }

    @Override
    public Object getHint(String key) {
        return null;
    }

    @Override
    public Map<String, Object> getHints() {
        return null;
    }

    @Override
    public boolean isHintSet(String key) {
        return false;
    }

    @Override
    public boolean isDefaultPUFetchGroupConfigurationOnly() {
        return false;
    }

    @Override
    public Set<Class<?>> getRootClasses() {
        return null;
    }

    @Override
    public FetchConfiguration setRootClasses(Collection<Class<?>> classes) {
        return null;
    }

    @Override
    public Set<Object> getRootInstances() {
        return null;
    }

    @Override
    public FetchConfiguration setRootInstances(Collection<?> roots) {
        return null;
    }

    @Override
    public void lock() {

    }

    @Override
    public void unlock() {

    }

    @Override
    public int requiresFetch(FieldMetaData fm) {
        return 0;
    }

    @Override
    public boolean requiresLoad() {
        return false;
    }

    @Override
    public FetchConfiguration traverse(FieldMetaData fm) {
        return null;
    }

    @Override
    public boolean isFetchConfigurationSQLCacheAdmissible() {
        return false;
    }
}
