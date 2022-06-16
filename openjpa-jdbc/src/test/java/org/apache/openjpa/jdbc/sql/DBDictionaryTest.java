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
package org.apache.openjpa.jdbc.sql;

import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.kernel.exps.Val;
import org.apache.openjpa.jdbc.sql.entities.NonSerializableDummy;
import org.apache.openjpa.kernel.BrokerImpl;
import org.apache.openjpa.util.StoreException;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.util.*;

import static junit.framework.TestCase.fail;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class DBDictionaryTest {

    @RunWith(Parameterized.class)
    public static class ToSnakeCaseTest {

        private DBDictionary dbDictionary;

        private String expected;
        private final String name;

        private Class<?> exception;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    //basic
                    {null},
                    {""},
                    {"test"},
                    {"tesT"},
                    {"TesT"},
                    {"AnoThEr_Test"}
            };

            return Arrays.asList(params);
        }

        public ToSnakeCaseTest(String param) {
            this.name = param;
        }

        @Before
        public void setUp() {
            this.dbDictionary = new DBDictionary();

            oracle();
        }

        private void oracle() {

            if(name == null) {
                this.exception = NullPointerException.class;

            }else if(name.equals("")){
                this.expected = "";

            }else{
                StringBuilder result = new StringBuilder();

                char c = name.charAt(0); //first character
                result.append(Character.toLowerCase(c));

                char prevCh = c;
                for (int i = 1; i < name.length(); i++) { //scan string

                    char ch = name.charAt(i);
                    if (Character.isUpperCase(ch)) {
                        if(!(prevCh == '_')){
                            result.append('_');
                        }
                        result.append(Character.toLowerCase(ch));
                    } else {
                        result.append(ch);
                    }
                    prevCh = ch;
                }

                this.expected = result.toString();
            }
        }

        @Test
        public void testToSnakeCase() {
            Assume.assumeNotNull(expected);

            try{
                String value = dbDictionary.toSnakeCase(name);

                Assert.assertEquals(expected, value);
            }catch (Exception e){
                fail("Should not throw an exception.");
            }
        }

        @Test
        public void testToSnakeCaseException() {
            Assume.assumeNotNull(exception);

            try{
                dbDictionary.toSnakeCase(name);

                fail("Should throw: " + exception);
            }catch (Exception e){
                Assert.assertEquals(exception, e.getClass());
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class SerializeTest {

        private DBDictionary dbDictionary;

        private final Object val;
        private final JDBCStore store;
        private byte[] expected;

        private Class<?> exception;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    {null, mock(JDBCStore.class)},
                    {"VAL", null},
                    {"VAL", mock(JDBCStore.class)},
                    {new NonSerializableDummy("dummy"), mock(JDBCStore.class)}
                    /*
                    {, SQLException.class},
                    {, StoreException.class},
                    {, NotSerializableException.class}
                     */

            };

            return Arrays.asList(params);
        }

        public SerializeTest(Object param1, JDBCStore param2) {
            this.val = param1;
            this.store = param2;
        }

        @Before
        public void setUp() {
            this.dbDictionary = new DBDictionary();

            if(store != null){
                when(store.getContext()).thenReturn(new BrokerImpl());
            }

            oracle();
        }

        private void oracle() {
            if(val == null){
                this.expected = null;
                return;
            }

            if(store == null){
                this.exception = NullPointerException.class;
                return;
            }

            if(val instanceof Serializable){
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                     ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                    oos.writeObject(val);

                    this.expected = bos.toByteArray();
                }catch(IOException e){
                    fail();
                }
            }else{
                this.exception = StoreException.class;
            }
        }

        @Test
        public void testSerialize() {

            try{
                byte[] serialized = dbDictionary.serialize(val, store);

                Assert.assertArrayEquals(expected, serialized);

            }catch(Exception e){
                Assert.assertEquals(exception, e.getClass());
            }
        }
    }

    /*
     * add CAST for a function operator where operand is a param
     * */
    @RunWith(Parameterized.class)
    public static class AddCastAsTypeTest {

        private DBDictionary dbDictionary;

        private final String func;
        private final Val val;

        private String expected;
        private Class<?> expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    {"func", null},
                    {null, mock(Val.class)},
                    {"", mock(Val.class)},
                    {"func", mock(Val.class)}
            };

            return Arrays.asList(params);
        }

        public AddCastAsTypeTest(String param1, Val param2) {
            this.func = param1;
            this.val = param2;
        }

        @Before
        public void setUp() {
            this.dbDictionary = new DBDictionary();

            oracle();
        }

        private void oracle() {
            if(func == null || val == null){
                this.expectedException = NullPointerException.class;
                return;
            }

            this.expected = func;
        }

        @Test
        public void testAddCastAsType() {
            Assume.assumeNotNull(expected);
            try{
                String result = dbDictionary.addCastAsType(func,val);
                Assert.assertEquals(expected, result);

            }catch(Exception e){
                fail("Should not throw an exception.");
            }

        }

        @Test
        public void testAddCastAsTypeException() {
            Assume.assumeNotNull(expectedException);

            try{
                dbDictionary.addCastAsType(func,val);
                fail("Should throw: " + expectedException);

            }catch(Exception e){
                Assert.assertEquals(expectedException, e.getClass());
            }

        }
    }

}
