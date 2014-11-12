/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.ec;

import java.util.Map;

public class ECSchema {

  private String schemaName;
  private String codecName;
  private Map<String, String> options;
  private int dataBlocks;
  private int parityBlocks;

  public ECSchema(String codecName, Map<String, String> options, String codec) {
    this.codecName = codecName;
    this.options = options;
    this.codecName = codec;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getCodecName() {
    return codecName;
  }

  /**
   * Erasure coding options to be configured and used by ErasureCoder.
   * @return
   */
  public Map<String, String> getOptions() {
    return options;
  }

  /**
   * Get required data blocks count in a BlockGroup,
   * to be called by ECManager when calculating BlockGroup.
   * @return
   */
  public int getDataBlocks() {
    return dataBlocks;
  }

  /**
   * Get required parity blocks count in a BlockGroup,
   * to be called by ECManager when calculating BlockGroup.
   * @return
   */
  public int getParityBlocks() {
    return parityBlocks;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setCodecName(String codecName) {
    this.codecName = codecName;
  }

  public void setOptions(Map<String, String> options) {
    this.options = options;
  }

  public void setDataBlocks(int dataBlocks) {
    this.dataBlocks = dataBlocks;
  }

  public void setParityBlocks(int parityBlocks) {
    this.parityBlocks = parityBlocks;
  }
}
