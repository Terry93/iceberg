/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.procedures;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class AuditBeforeWriteProcedure extends BaseProcedure {
  private AuditBeforeWriteProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return new ProcedureParameter[0];
  }

  @Override
  public StructType outputType() {
    return new StructType(
        new StructField[] {
          new StructField("added_files_count", DataTypes.StringType, false, Metadata.empty())
        });
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    // CALL spark_catalog.systems.audit_before_write(
    //  select => 'select * from query_analytics.queries where dt = '2023-06-10'',
    //  write => 'insert overwrite x.y partition (col = '2023-06-10')',
    //  audit_class => 'com.pinterest.pintel.s3dataset.audit.S3DatasetExampleAuditor'
    // );

    // step 1: get select statement
    String select_statement = args.getString(0);
    Dataset<Row> df = spark().sql(select_statement);

    // step 2: get audit class
    String audit_class = args.getString(2);
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Class<?> loadedClass = classLoader.loadClass(audit_class);
    Object instance = loadedClass.newInstance();
    Method method = loadedClass.getDeclaredMethod("audit", Dataset.class);
    Optional<String> result = (Optional<String>) method.invoke(instance, df);
    if (!result.isPresent()) {
      throw new RuntimeException("Audit failed");
    }

    // step 3: write after audit passed
    String write_statement = args.getString(1);
    df.registerTempTable("tmp_tbl");
    spark().sql(write_statement + " select * from tmp_tbl");
    String audit_message = result.get();
    return new InternalRow[] {newInternalRow(UTF8String.fromString(audit_message))};
  }

  @Override
  public String description() {
    return super.description();
  }

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<AuditBeforeWriteProcedure>() {
      @Override
      protected AuditBeforeWriteProcedure doBuild() {
        return new AuditBeforeWriteProcedure(tableCatalog());
      }
    };
  }
}
