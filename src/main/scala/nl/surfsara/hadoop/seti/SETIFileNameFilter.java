package nl.surfsara.hadoop.seti;

import java.io.File;
import java.io.FilenameFilter;

public class SETIFileNameFilter extends FilenameFilter {

    @Override
    public boolean accept(File dir, String name) {
        return false;
    }
}
