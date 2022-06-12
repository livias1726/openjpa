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

import org.apache.openjpa.kernel.entities.DummyEntity;
import org.apache.openjpa.persistence.*;
import org.apache.openjpa.util.IntId;
import org.apache.openjpa.util.UserException;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.persistence.*;
import java.util.*;

import static junit.framework.TestCase.fail;

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

        //test
        private boolean persistFailed;

        private Object[] expected;
        private Class<?> expectedException;

        private int expectedId;
        private String expectedName;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    {null, true, fcb}, //expected NullPointerException
                    {Collections.EMPTY_LIST, false, fcb}, //expected empty list
                    {new ArrayList<>(Collections.singleton(null)), false, fcb}, //expected UserException
                    {new ArrayList<>(Collections.singleton(new IntId(DummyEntity.class, String.valueOf(1)))), true, null},
            };

            return Arrays.asList(params);
        }

        public FindAllTest(Collection<IntId> param1, boolean param2, FindCallbacks param3){
            this.oids = param1;
            this.validate = param2;
            this.call = param3;
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

            oracle();
        }

        private void oracle() {

            if(oids == null){
                this.expected = null;
                this.expectedException = NullPointerException.class;

            }else if(oids.isEmpty()){
                this.expected = new Object[]{};
                this.expectedException = null;

            }else if(oids.contains(null)) {
                this.expected = null;
                this.expectedException = UserException.class;

            }else{
                DummyEntity de = new DummyEntity("entity", 1);
                broker.persist(de, pcb);

                if(persistFailed){
                    this.expected = new Object[]{null};
                }else{
                    this.expected = new Object[]{de};
                    this.expectedId = de.getId();
                    this.expectedName = de.getName();
                }
            }

        }

        OpCallbacks pcb = (op, arg, sm) -> {
            this.persistFailed = sm == null;

            return op;
        };

        @Test
        public void testFindAllWithPersistence(){
            Assume.assumeFalse(persistFailed || expected != null || expectedException != null);

            Object[] res = broker.findAll(oids, validate, call);
            Assert.assertNotNull(res);
            Assert.assertEquals(expected.length, res.length);
            Assert.assertArrayEquals(expected, res);

            DummyEntity resEnt = (DummyEntity)res[0];
            Assert.assertEquals(expectedId, resEnt.getId());
            Assert.assertEquals(expectedName, resEnt.getName());
        }

        @Test
        public void testFindAllWithoutPersistence(){
            Assume.assumeTrue(persistFailed);

            Object[] res = broker.findAll(oids, validate, call);
            Assert.assertNotNull(res);
            Assert.assertArrayEquals(expected, res);
        }

        @Test
        public void testFindAllEmpty(){
            Assume.assumeTrue(expected != null && expected.length == 0);
            Object[] res = broker.findAll(oids, validate, call);
            Assert.assertNotNull(res);

            Assert.assertEquals(0, res.length);
            Assert.assertArrayEquals(expected, res);
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
    public static class AddTransactionalListenerTest {

        private BrokerImpl broker;

        private final Object classes;

        public Boolean expected;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    {new Class[]{DummyEntity.class}},
                    {new Class[]{}},
                    {null}
            };

            return Arrays.asList(params);
        }

        public AddTransactionalListenerTest(Object param){
            this.classes = param;
        }

        @Before
        public void setUp() {
            OpenJPAEntityManagerFactorySPI emf = (OpenJPAEntityManagerFactorySPI)OpenJPAPersistence
                    .cast(Persistence.createEntityManagerFactory("isw2-tests"));

            this.broker = (BrokerImpl) JPAFacadeHelper.toBroker(emf.createEntityManager());

            oracle();
        }

        private void oracle() {
            this.expected = (classes == null);
        }

        @Test
        public void testAddTransactionalListener(){
            if(broker == null){
                fail("Broker instrumentation failed.");
            }

            Collection<Object> res = broker.getTransactionListeners();
            Assert.assertTrue(res.isEmpty());

            broker.addTransactionListener(classes);

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

        private final boolean val;
        private boolean expected;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    {true},
                    {false}
            };

            return Arrays.asList(params);
        }

        public SetOptimisticTest(boolean param){
            this.val = param;
        }

        @Before
        public void setUp() {

            OpenJPAEntityManagerFactorySPI emf = (OpenJPAEntityManagerFactorySPI)
                    OpenJPAPersistence.cast(
                            Persistence.createEntityManagerFactory("isw2-tests"));

            this.broker = (BrokerImpl) JPAFacadeHelper.toBroker(emf.createEntityManager());

            oracle(true);
        }

        private void oracle(boolean fromSetUp) {
            if(fromSetUp){
                this.expected = val;
            }else{
                this.expected = !val;
            }
        }

        @Test
        public void testSetOptimistic(){
            if(broker == null){
                fail("Failed to initialize broker.");
            }

            if(broker.getOptimistic() == val){
                broker.setOptimistic(!val);
                oracle(false);
            }else{
                broker.setOptimistic(val);
            }

            Assert.assertEquals(expected, broker.getOptimistic());
        }

        @After
        public void tearDown(){
            broker.close();
        }
    }

}
