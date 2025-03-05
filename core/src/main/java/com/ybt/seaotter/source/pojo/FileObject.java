package com.ybt.seaotter.source.pojo;

import com.ybt.seaotter.source.pojo.enums.FileType;

public class FileObject {
    String name;
    FileType  type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FileType getType() {
        return type;
    }

    public void setType(FileType type) {
        this.type = type;
    }
}
