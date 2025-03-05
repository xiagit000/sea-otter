package com.ybt.seaotter.source.meta.file;

import com.ybt.seaotter.source.pojo.FileObject;

import java.util.List;

public interface DirMeta {

    List<FileObject> list(String dir, List<String> formats);
    FileMeta path(String fileName);
}
