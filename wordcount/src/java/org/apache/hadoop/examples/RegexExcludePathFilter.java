package org.apache.hadoop.examples;

import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexExcludePathFilter   implements PathFilter {
	Pattern pattern;

	@Override
	public boolean accept(Path path) {
		System.out.println("path.toString()"+path.toString());
		System.out.println("path.getName()"+path.getName());
		return path.getName().startsWith("file");
	}
}