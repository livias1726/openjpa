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

import org.apache.openjpa.jdbc.kernel.exps.FilterValue;

/**
 * Base dictionary for the IBM DB2 family of databases.
 */
public abstract class AbstractDB2Dictionary
    extends DBDictionary {

    public int varcharCastLength = 1000;

    public AbstractDB2Dictionary() {
    	// http://publib.boulder.ibm.com/infocenter/db2luw/v9/index.jsp?topic=/com.ibm.db2.udb.apdv.java.doc
    	//	/doc/rjvjdata.htm
		// http://db.apache.org/derby/docs/10.3/ref/rrefsqlj12362.html
    	numericTypeName = "DECIMAL";
        bitTypeName = "SMALLINT";
        smallintTypeName = "SMALLINT";
        tinyintTypeName = "SMALLINT";
        longVarbinaryTypeName = "BLOB";
        varbinaryTypeName = "BLOB";

        // DB2-based databases have restrictions on having uncast parameters
        // in string functions
        stringLengthFunction = "LENGTH({0})";
        concatenateFunction = "(CAST({0} AS VARCHAR(" + varcharCastLength
            + ")) || CAST({1} AS VARCHAR(" + varcharCastLength + ")))";

        trimLeadingFunction = "LTRIM({0})";
        trimTrailingFunction = "RTRIM({0})";
        trimBothFunction = "LTRIM(RTRIM({0}))";

        // in DB2, "for update" seems to be ignored with isolation
        // levels below REPEATABLE_READ... force isolation to behave like RR
        forUpdateClause = "FOR UPDATE WITH RR";

        supportsLockingWithDistinctClause = false;
        supportsLockingWithMultipleTables = false;
        supportsLockingWithOrderClause = false;
        supportsLockingWithOuterJoin = false;
        supportsLockingWithInnerJoin = false;
        supportsLockingWithSelectRange = true;
        supportsCaseConversionForLob = true;

        requiresAutoCommitForMetaData = true;
        requiresAliasForSubselect = true;

        supportsAutoAssign = true;
        autoAssignClause = "GENERATED BY DEFAULT AS IDENTITY";
        lastGeneratedKeyQuery = "VALUES(IDENTITY_VAL_LOCAL())";

        // DB2 doesn't understand "X CROSS JOIN Y", but it does understand
        // the equivalent "X JOIN Y ON 1 = 1"
        crossJoinClause = "JOIN";
        requiresConditionForCrossJoin = true;
        
        // DB2 / Derby allows precision to be set on a DECIMAL column.
        fixedSizeTypeNameSet.remove("DECIMAL");
    }

    public void indexOf(SQLBuffer buf, FilterValue str, FilterValue find,
        FilterValue start) {
        buf.append("(LOCATE(CAST((");
        find.appendTo(buf);
        buf.append(") AS VARCHAR(").append(Integer.toString(varcharCastLength))
            .append(")), CAST((");
        str.appendTo(buf);
        buf.append(") AS VARCHAR(").append(Integer.toString(varcharCastLength))
            .append("))");
        if (start != null) {
            buf.append(", CAST((");
            start.appendTo(buf);
            buf.append(") AS INTEGER) + 1");
        }
        buf.append(") - 1)");
    }

    public void substring(SQLBuffer buf, FilterValue str, FilterValue start,
        FilterValue end) {
        buf.append("SUBSTR(CAST((");
        str.appendTo(buf);
        buf.append(") AS VARCHAR(").append(Integer.toString(varcharCastLength))
            .append(")), ");
        if (start.getValue() instanceof Number) {
            long startLong = toLong(start);
            buf.append(Long.toString(startLong + 1));
        } else {
            buf.append("CAST((");
            start.appendTo(buf);
            buf.append(") AS INTEGER) + 1");
        }
        if (end != null) {
            buf.append(", ");
            if (start.getValue() instanceof Number
                && end.getValue() instanceof Number) {
                long startLong = toLong(start);
                long endLong = toLong(end);
                buf.append(Long.toString(endLong - startLong));
            } else {
                buf.append("CAST((");
                end.appendTo(buf);
                buf.append(") AS INTEGER) - CAST((");
                start.appendTo(buf);
                buf.append(") AS INTEGER)");
            }
        }
        buf.append(")");
    }
}
