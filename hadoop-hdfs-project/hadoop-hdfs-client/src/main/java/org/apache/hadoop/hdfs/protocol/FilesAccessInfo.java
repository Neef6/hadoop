package org.apache.hadoop.hdfs.protocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FilesAccessInfo {
  private List<String> filesAccessed;
  private List<Integer> filesAccessCounts;
  private List<NNEvent> nnEvents;

  public FilesAccessInfo() {
  }

  public void setAccessCounter(Map<String, Integer> countsMap) {
    this.filesAccessed = new ArrayList<>(countsMap.keySet());
    this.filesAccessCounts = new ArrayList<>(countsMap.values());
  }

  public void setAccessCounter(List<String> files, List<Integer> counts) {
    filesAccessed = files;
    filesAccessCounts = counts;
  }

  public Map<String, Integer> getFilesAccessedHashMap() {
    if (filesAccessed == null || filesAccessCounts == null) {
      return null;
    }
    Map<String, Integer> ret = new HashMap<>();
    for (int i = 0; i < filesAccessed.size(); i++) {
      ret.put(filesAccessed.get(i), filesAccessCounts.get(i));
    }
    return ret;
  }

  public List<String> getFilesAccessed() {
    return filesAccessed;
  }

  public List<Integer> getFilesAccessCounts() {
    return filesAccessCounts;
  }

  public void setNnEvents(List<NNEvent> events) {
    this.nnEvents = events;
  }

  public List<NNEvent> getNnEvents() {
    return nnEvents;
  }
}