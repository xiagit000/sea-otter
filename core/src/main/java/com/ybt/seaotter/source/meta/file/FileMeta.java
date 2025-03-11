package com.ybt.seaotter.source.meta.file;

import com.ybt.seaotter.source.pojo.FileObject;

import java.util.List;

public interface FileMeta {

    List<String> columns();
    List<List<String>> rows(Integer limit);
    FileMeta separator(String separator);
}
