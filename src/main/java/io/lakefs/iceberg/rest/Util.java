package io.lakefs.iceberg.rest;

import org.apache.commons.lang3.StringUtils;

public class Util {

    private Util() {}

    /**
     * Given a full lakeFS URI, return the path relative to the repository root.
     * For example, given <code>lakefs://example-repo/main/a/b/c</code>, return <code>a/b/c</code>.
     */
    public static String GetPathFromURL(String lakeFSLocation){
        // return sub-string after lakeFS ref
        return StringUtils.substring(lakeFSLocation, StringUtils.ordinalIndexOf(lakeFSLocation, "/", 4) + 1);
    }

}
