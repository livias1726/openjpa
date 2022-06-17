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
import org.apache.openjpa.jdbc.meta.JavaSQLTypes;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.jdbc.sql.entities.NonSerializableDummy;
import org.apache.openjpa.kernel.BrokerImpl;
import org.apache.openjpa.meta.JavaTypes;
import org.apache.openjpa.util.StoreException;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.verification.VerificationMode;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
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
                    {"AnoThEr_Test"},
                    //new
                    {"_test"},
                    {"_Test"},
                    {"$dollarTest$"},
                    {"questionTest?"},
                    {"\"quotedTest\""},

                    //mutation
                    {"\"QuotedTest\""}, //is delimited but starts with uppercase
                    {"bT"}
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

            } else{
                StringBuilder result = new StringBuilder();

                int i = 0;
                char c = name.charAt(i); //first character
                System.out.println(name.charAt(i));
                if(c == dbDictionary.getLeadingDelimiter().charAt(0)){
                    result.append(c).append(Character.toLowerCase(name.charAt(++i)));
                    System.out.println(name.charAt(i));
                }else{
                    result.append(Character.toLowerCase(c));
                }

                char prevCh = name.charAt(i++); //first character
                System.out.println(prevCh);
                for (int j = i; j < name.length(); j++) { //scan string

                    char ch = name.charAt(j);
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
                    {new NonSerializableDummy("dummy"), mock(JDBCStore.class)},

                    //mutation
                    {new DBDictionary.SerializedData("val".getBytes(StandardCharsets.UTF_8)), mock(JDBCStore.class)}
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
            }else if(val instanceof DBDictionary.SerializedData){
                this.expected = ((DBDictionary.SerializedData) val).bytes;

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

    /*
    * Set a column value into a prepared statement.
        Parameters:
            stmnt - the prepared statement to parameterize
            idx - the index of the parameter in the prepared statement
            val - the value of the column
            col - the column being set
            type - the field mapping type code for the value
            store - the store manager for the current context
        Throws:
            SQLException
    * */
    @RunWith(Parameterized.class)
    public static class SetTypedTest {

        private DBDictionary dbDictionary;
        private static final PreparedStatement mockStatement = mock(PreparedStatement.class);

        private static final int[] types = {JavaTypes.STRING, JavaSQLTypes.SQL_DATE, JavaSQLTypes.TIME,
                JavaSQLTypes.TIMESTAMP};

        private final PreparedStatement stmnt;
        private final int idx;

        private final Object val;
        private String valString;
        private Date valDate;
        private Time valTime;
        private Timestamp valTimestamp;

        private final Column col;
        private final int type;
        private final JDBCStore store;

        private Map<Integer, VerificationMode> expectedVerify;
        private Class<?> exception;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    {null, 0, null, null, -1, null}, //null pointer
                    {mockStatement, 1, "val", null, types[0], null}, //string
                    {mockStatement, 2, new Date(System.currentTimeMillis()), new Column(), types[1], null},
                    {mockStatement, -1, new Time(System.currentTimeMillis()), new Column(), types[2], null},
                    {mockStatement, -2, new Timestamp(System.currentTimeMillis()), new Column(), types[3], null},
                    {mockStatement, 10, "val", null, types[2], mock(JDBCStore.class)}, //class cast
            };

            return Arrays.asList(params);
        }

        public SetTypedTest(PreparedStatement param1, int param2, Object param3, Column param4, int param5, JDBCStore param6) {
            this.stmnt = param1;
            this.idx = param2;
            this.val = param3;
            this.type = param5;
            this.col = param4;

            this.store = param6;
        }

        @Before
        public void setUp() {
            this.dbDictionary = new DBDictionary();
            dbDictionary.useSetStringForClobs = true; //for mutation

            try{
                setParams();
            }catch(Exception e) {
                oracle(e);
            }

            oracle(null);
        }

        private void setParams() throws ClassCastException{
            switch(type){
                case JavaTypes.STRING:
                    valString = (String) val;
                    break;
                case JavaSQLTypes.SQL_DATE:
                    valDate = (Date) val;
                    break;
                case JavaSQLTypes.TIME:
                    valTime = (Time) val;
                    break;
                case JavaSQLTypes.TIMESTAMP:
                    valTimestamp = (Timestamp) val;
                    break;
                default:
                    break;
            }
        }

        private void oracle(Exception e) {

            if(e != null){
                this.exception = e.getClass();
                return;
            }

            if(stmnt == null){
                this.exception = NullPointerException.class;
                return;
            }

            expectedVerify = new HashMap<>();
            for(int type: types){
                if(type == this.type){
                    expectedVerify.put(type, atMostOnce());
                }else{
                    expectedVerify.put(type, never());
                }
            }

        }

        @Test
        public void testSetTyped() {
            Assume.assumeTrue(exception == null);

            try{
                dbDictionary.setTyped(stmnt, idx, val, col, type, store);

                verify(mockStatement, expectedVerify.get(type)).setString(idx, valString);
                verify(mockStatement, expectedVerify.get(type)).setDate(idx, valDate);
                verify(mockStatement, expectedVerify.get(type)).setTime(idx, valTime);
                verify(mockStatement, expectedVerify.get(type)).setTimestamp(idx, valTimestamp);

            }catch (Exception e){
                fail("Should not throw " + e.getClass());
            }
        }

        @Test
        public void testSetTypedException() {
            Assume.assumeNotNull(exception);

            try{
                dbDictionary.setTyped(stmnt, idx, val, col, type, store);
                fail("Should throw " + exception);
            }catch (Exception e){
                Assert.assertEquals(exception, e.getClass());
            }
        }
    }

}
