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
package org.apache.openjpa.kernel;

import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.kernel.entities.DummyEntity;
import org.apache.openjpa.kernel.entities.DummyFetchConfiguration;
import org.apache.openjpa.kernel.entities.DummyListener;
import org.apache.openjpa.persistence.*;
import org.apache.openjpa.util.*;
import org.apache.openjpa.util.InvalidStateException;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import javax.persistence.*;
import java.util.*;

import static junit.framework.TestCase.fail;
import static org.apache.openjpa.kernel.StoreContext.*;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class BrokerImplTest {

    @RunWith(Parameterized.class)
    public static class FindAllTest {

        //setup
        private BrokerImpl broker;

        //params
        private final Collection<IntId> oids;
        private final boolean validate;
        private final FindCallbacks call;

        private final boolean nonTransRead;

        private Object[] expected;
        private Class<?> expectedException;

        private Object[] expectedId;
        private Object[] expectedName;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    //base
                    {null, true, fcb, true}, //expected NullPointerException
                    {Collections.EMPTY_LIST, false, fcb, true}, //expected empty list
                    {new ArrayList<>(Collections.singleton(null)), false, fcb, true}, //expected UserException
                    {new ArrayList<>(Collections.singleton(new IntId(DummyEntity.class, String.valueOf(1)))), true, null, true},
                    {new ArrayList<>(Collections.singleton(new IntId(DummyEntity.class, String.valueOf(1)))), false, null, true},
                    {new ArrayList<>(Arrays.asList(new IntId(DummyEntity.class, String.valueOf(1)),
                            new IntId(DummyEntity.class, String.valueOf(2)))), false, null, true},

                    //mutation
                    {Collections.EMPTY_LIST, false, fcb, false}, //expected empty list
                    {new ArrayList<>(Collections.singleton(new IntId(DummyEntity.class, String.valueOf(1)))), true, null, false},
                    {new ArrayList<>(Collections.singleton(new IntId(DummyEntity.class, String.valueOf(1)))), false, null, false},
                    {new ArrayList<>(Arrays.asList(new IntId(DummyEntity.class, String.valueOf(1)),
                            new IntId(DummyEntity.class, String.valueOf(2)))), false, null, false},
            };

            return Arrays.asList(params);
        }

        public FindAllTest(Collection<IntId> param1, boolean param2, FindCallbacks param3, boolean param4){
            this.oids = param1;
            this.validate = param2;
            this.call = param3;

            this.nonTransRead = param4;
        }

        @Before
        public void setUp() {

            OpenJPAEntityManagerFactorySPI emf = (OpenJPAEntityManagerFactorySPI)
                    OpenJPAPersistence.cast(
                            Persistence.createEntityManagerFactory("isw2-tests"));

            OpenJPAEntityManagerSPI em = emf.createEntityManager();

            this.broker = (BrokerImpl) JPAFacadeHelper.toBroker(em);
            if(broker == null){
                fail("Broker instrumentation failed.");
            }

            broker.setNontransactionalRead(nonTransRead);

            oracle();
        }

        private void oracle() {

            if ((this.expectedException = setExpectedException()) != null) {
                this.expected = null;
                return;
            }

            if (oids.isEmpty()) {
                this.expected = new Object[]{};
                return;
            }

            DummyEntity de;
            List<Object> listExp = new ArrayList<>();
            List<Object> listExpId = new ArrayList<>();
            List<Object> listExpName = new ArrayList<>();
            for(IntId oid: oids) {
                if(validate || oid == null){
                    listExp.add(null);
                    listExpId.add(null);
                    listExpName.add(null);
                }else {
                    de = new DummyEntity("entity" + oid.getId(), oid.getId());
                    broker.persist(de, null);

                    listExp.add(de);
                    listExpId.add(de.getId());
                    listExpName.add(de.getName());
                }
            }

            this.expected = listExp.toArray();
            this.expectedId = listExpId.toArray();
            this.expectedName = listExpName.toArray();
        }

        private Class<?> setExpectedException() {
            Class<?> exception = null;

            if(oids == null){
                exception = NullPointerException.class;

            }else if(oids.contains(null) && !validate) {
                exception = UserException.class;

            }else if(!nonTransRead){
                exception = InvalidStateException.class;
            }

            return exception;
        }

        @Test
        public void testFindAll(){
            Assume.assumeTrue(expectedException == null);

            try{
                Object[] res = broker.findAll(oids, validate, call);
                Assert.assertNotNull(res);
                Assert.assertEquals(expected.length, res.length);
                Assert.assertArrayEquals(expected, res);

                for(int i=0; i<res.length; i++){
                    DummyEntity resEnt = (DummyEntity)res[i];
                    if(resEnt == null){
                        continue;
                    }

                    Assert.assertEquals(expectedId[i], resEnt.getId());
                    Assert.assertEquals(expectedName[i], resEnt.getName());
                }
            }catch(Exception e){
                fail("Should not throw an exception. " + e.getClass());
            }

        }

        @Test
        public void testFindAllException(){
            Assume.assumeNotNull(expectedException);

            try{
                broker.findAll(oids, validate, call);
                fail("Should have triggered " + expectedException);
            }catch(Exception e){
                Assert.assertEquals(expectedException, e.getClass());
            }
        }

        @After
        public void tearDown(){
            broker.close();
        }

        static FindCallbacks fcb = new FindCallbacks() {
            @Override
            public Object processArgument(Object oid) {
                return null;
            }

            @Override
            public Object processReturn(Object oid, OpenJPAStateManager sm) {
                return null;
            }
        };
    }

    @RunWith(Parameterized.class)
    public static class FindAll2Test {

        //setup
        private BrokerImpl broker;
        private static final int defaultFlags = OID_COPY | OID_ALLOW_NEW | OID_NODELETED;
        private static final int flagsWithNoValidate = defaultFlags | OID_NOVALIDATE;
        private static final FetchConfiguration defaultFetch = new DummyFetchConfiguration();

        //params
        private final Collection<IntId> oids;
        private FetchConfiguration fetch;
        private final BitSet exclude;
        private final Object edata;
        private final int flags;

        private Object[] expected;
        private Class<?> expectedException;

        private Object[] expectedId;
        private Object[] expectedName;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    //replication of FindAllTest
                    {null, defaultFetch, null, null, flagsWithNoValidate}, //expected NullPointerException
                    {Collections.EMPTY_LIST, defaultFetch, null, null, defaultFlags}, //expected empty list
                    {new ArrayList<>(Collections.singleton(null)), defaultFetch, null, null, defaultFlags}, //expected UserException
                    {new ArrayList<>(Collections.singleton(new IntId(DummyEntity.class, String.valueOf(1)))),
                            defaultFetch, null, null, flagsWithNoValidate},

                    //new tests
                    {Collections.EMPTY_LIST, null, null, null, defaultFlags}, //expected empty list
                    {new ArrayList<>(Collections.singleton(new IntId(DummyEntity.class, String.valueOf(1)))),
                            defaultFetch, null, null, defaultFlags},
                    {new ArrayList<>(Collections.singleton(new IntId(DummyEntity.class, String.valueOf(1)))),
                            defaultFetch, new BitSet(), null, defaultFlags},
                    {new ArrayList<>(Collections.singleton(new IntId(DummyEntity.class, String.valueOf(1)))),
                            defaultFetch, EXCLUDE_ALL, null, flagsWithNoValidate},
                    {new ArrayList<>(Arrays.asList(new IntId(DummyEntity.class, String.valueOf(1)),
                            new IntId(DummyEntity.class, String.valueOf(2)))), null, null, null, defaultFlags},

            };

            return Arrays.asList(params);
        }

        public FindAll2Test(Collection<IntId> param1, FetchConfiguration param2, BitSet param3, Object param4, int param5){
            this.oids = param1;
            this.fetch = param2;
            this.exclude = param3;
            this.edata = param4;
            this.flags = param5;
        }

        @Before
        public void setUp() {

            OpenJPAEntityManagerFactorySPI emf = (OpenJPAEntityManagerFactorySPI)
                    OpenJPAPersistence.cast(
                            Persistence.createEntityManagerFactory("isw2-tests"));

            OpenJPAEntityManagerSPI em = emf.createEntityManager();

            this.broker = (BrokerImpl) JPAFacadeHelper.toBroker(em);
            if(broker == null){
                fail("Broker instrumentation failed.");
            }

            if(fetch != null && fetch.equals(defaultFetch)){
                fetch = broker.getFetchConfiguration();
            }

            oracle();
        }

        private void oracle() {

            if((this.expectedException = setExpectedException()) != null){
                return;
            }

            DummyEntity de;
            List<Object> listExp = new ArrayList<>();
            List<Object> listExpId = new ArrayList<>();
            List<Object> listExpName = new ArrayList<>();
            for(IntId oid: oids){
                if(flags == defaultFlags || oid == null){
                    listExp.add(null);
                    listExpId.add(null);
                    listExpName.add(null);

                }else{
                    de = new DummyEntity("entity" + oid.getId(), oid.getId());

                    broker.persist(de, null);

                    listExp.add(de);
                    listExpId.add(de.getId());
                    listExpName.add(de.getName());
                }
            }

            this.expected = listExp.toArray();
            this.expectedId = listExpId.toArray();
            this.expectedName = listExpName.toArray();
        }

        private Class<?> setExpectedException() {
            Class<?> exception = null;

            if(oids == null){
                exception = NullPointerException.class;

            }else if(oids.contains(null) && flags == flagsWithNoValidate) {

                exception = UserException.class;
            }

            return exception;
        }

        @Test
        public void testFindAll(){
            Assume.assumeTrue(expectedException == null);

            try{
                Object[] res = broker.findAll(oids, fetch, exclude, edata, flags);
                Assert.assertNotNull(res);
                Assert.assertEquals(expected.length, res.length);
                Assert.assertArrayEquals(expected, res);

                for(int i=0; i<res.length; i++){
                    DummyEntity resEnt = (DummyEntity)res[i];
                    if(resEnt == null){
                        continue;
                    }

                    Assert.assertEquals(expectedId[i], resEnt.getId());
                    Assert.assertEquals(expectedName[i], resEnt.getName());
                }

            }catch(Exception e){
                fail("Should not throw exception.");
            }
        }

        @Test
        public void testFindAllException(){
            Assume.assumeNotNull(expectedException);

            try{
                broker.findAll(oids, fetch, exclude, edata, flags);
                fail("Should have triggered " + expectedException);
            }catch(Exception e){
                Assert.assertEquals(expectedException, e.getClass());
            }
        }

        @After
        public void tearDown(){
            broker.close();
        }
    }

    @RunWith(Parameterized.class)
    public static class AddTransactionListenerTest {

        private BrokerImpl broker;

        private final Object tl;

        public Boolean expected;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    {new Class[]{DummyEntity.class}},
                    {new Class[]{}},
                    {null},
                    {new DummyListener(mock(OpenJPAConfiguration.class))}
            };

            return Arrays.asList(params);
        }

        public AddTransactionListenerTest(Object param){
            this.tl = param;
        }

        @Before
        public void setUp() {
            OpenJPAEntityManagerFactorySPI emf = (OpenJPAEntityManagerFactorySPI)OpenJPAPersistence
                    .cast(Persistence.createEntityManagerFactory("isw2-tests"));

            this.broker = (BrokerImpl) JPAFacadeHelper.toBroker(emf.createEntityManager());
            if(broker == null){
                fail("Broker instrumentation failed.");
            }


            oracle();
        }

        private void oracle() {
            this.expected = (tl == null);
        }

        @Test
        public void testAddTransactionListener(){
            Collection<Object> res = broker.getTransactionListeners();
            Assert.assertTrue(res.isEmpty());

            broker.addTransactionListener(tl);

            res = broker.getTransactionListeners();
            Assert.assertEquals(expected, res.isEmpty());
        }

        @After
        public void tearDown(){
            broker.close();
        }
    }

    @RunWith(Parameterized.class)
    public static class SetOptimisticTest {

        private BrokerImpl broker;
        private final int setFlag;
        private final boolean optOptimistic;
        private final boolean managed;
        private final boolean close;

        private final boolean val;

        private boolean expected;
        private Class<?> expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    //base
                    {true, 0, true, false, false},
                    {false, 0, true, false, false},

                    //upgrade
                    {true, 2, true, true, false}, //InvalidState
                    {false, 2, true, true, false}, //InvalidState
                    {true, 0, false, false, false}, //Unsupported

                    //mutation
                    {true, 0, true, false, true},
                    {false, 0, true, false, true},
            };

            return Arrays.asList(params);
        }

        public SetOptimisticTest(boolean param1, int param2, boolean param3, boolean param4, boolean param5){
            this.val = param1;

            this.setFlag = param2;
            this.optOptimistic = param3;
            this.managed = param4;
            this.close = param5;
        }

        @Before
        public void setUp() {

            OpenJPAEntityManagerFactorySPI emf = (OpenJPAEntityManagerFactorySPI)
                    OpenJPAPersistence.cast(
                            Persistence.createEntityManagerFactory("isw2-tests"));

            this.broker = (BrokerImpl) JPAFacadeHelper.toBroker(emf.createEntityManager());
            if(broker == null){
                fail("Failed to initialize broker.");
            }

            setNewInitialization();
            broker.setStatusFlag(setFlag);

            //mutation on assertOpen()
            if(close){
                broker.close();
            }

            oracle(true);
        }

        private void setNewInitialization() {
            Collection<String> supportedOptions = broker.getConfiguration().supportedOptions();
            if(!optOptimistic){
                supportedOptions.remove(OpenJPAConfiguration.OPTION_OPTIMISTIC);
            }else{
                if(!supportedOptions.contains(OpenJPAConfiguration.OPTION_OPTIMISTIC)){
                    supportedOptions.add(OpenJPAConfiguration.OPTION_OPTIMISTIC);
                }
            }

            OpenJPAConfiguration conf = Mockito.mock(OpenJPAConfiguration.class);
            when(conf.supportedOptions()).thenReturn(supportedOptions);

            AbstractBrokerFactory fact = Mockito.mock(AbstractBrokerFactory.class);
            when(fact.getConfiguration()).thenReturn(conf);

            DelegatingStoreManager sm = broker.getStoreManager();

            //overriding factory initialization
            broker.initialize(fact, sm, managed, ConnectionRetainModes.CONN_RETAIN_TRANS, true);
        }

        private void oracle(boolean fromSetUp) {
            if(fromSetUp){
                this.expectedException = setExpectedExceptions();

                this.expected = val;
            }else{
                this.expected = !val;
            }
        }

        private Class<?> setExpectedExceptions() {
            Class<?> exception = null;

            if(broker.isClosed()){
                exception = InvalidStateException.class;
            }else if(!optOptimistic){
                exception = UnsupportedException.class;
            }else if(setFlag == 2){
                exception = InvalidStateException.class;
            }

            return exception;
        }

        @Test
        public void testSetOptimistic(){
            Assume.assumeTrue(expectedException == null);

            try{
                if(broker.getOptimistic() == val){
                    broker.setOptimistic(!val);
                    oracle(false);
                }else{
                    broker.setOptimistic(val);
                }

                Assert.assertEquals(expected, broker.getOptimistic());

            }catch(Exception e){

                fail();
            }
        }

        @Test
        public void testSetOptimisticException(){
            Assume.assumeNotNull(expectedException);

            try{
                broker.setOptimistic(val);
                fail();

            }catch(Exception e){

                Assert.assertEquals(expectedException, e.getClass());
            }
        }

        @After
        public void tearDown(){
            if(broker.isClosed()){
                return;
            }

            try{
                broker.close();
            }catch(InvalidStateException e){
                broker.setStatusFlag(0);
                broker.close();
            }

        }
    }

}
