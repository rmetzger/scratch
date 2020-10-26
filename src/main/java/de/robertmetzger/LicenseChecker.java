package de.robertmetzger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Hello world!
 *
 */
public class LicenseChecker {
    private static final Logger LOG = LoggerFactory.getLogger(LicenseChecker.class);

    public static void main( String[] args ) throws IOException {
        LicenseChecker checker = new LicenseChecker();
        int issueCount = checker.run(new File(args[0]), new File(args[1]));

        if( issueCount > 0) {
            LOG.warn("Found a total of {} license issues", issueCount);

            System.exit(1);
        }
    }

    private final static Pattern INCLUDE_MODULE_PATTERN = Pattern.compile(".*Including ([^:]+):([^:]+):jar:([^ ]+) in the shaded jar");
    private static final Pattern NEXT_MODULE_PATTERN = Pattern.compile(".*:shade \\(shade-flink\\) @ ([^ _]+)(_[0-9.]+)? --.*");
    private final static Pattern NOTICE_DEPENDENCY_PATTERN = Pattern.compile("- ([^:]+):([^:]+):([^\n]+)$");

    private int run(File buildResult, File root) throws IOException {
        int issueCount = 0;
        // parse included dependencies from build output
        Multimap<String, Dependency> modulesWithShadedDependencies = parseModulesFromBuildResult(buildResult);
        LOG.info("Extracted " + modulesWithShadedDependencies.asMap().keySet().size() + " modules with a total of " + modulesWithShadedDependencies.values().size() + " dependencies");

        // find modules producing a shaded-jar
        List<File> noticeFiles = findNoticeFiles(root);
        LOG.info("Found {} NOTICE files to check", noticeFiles.size());

        // check that all required NOTICE files exists
        issueCount += ensureRequiredNoticeFiles(modulesWithShadedDependencies, noticeFiles);

        // check each NOTICE file
        for (File noticeFile: noticeFiles) {
            LOG.info("Checking NOTICE file {}", noticeFile.toString());
            issueCount += checkNoticeFile(modulesWithShadedDependencies, noticeFile);
        }

        // find modules included in flink-dist

        return issueCount;
    }

    private int ensureRequiredNoticeFiles(Multimap<String, Dependency> modulesWithShadedDependencies, List<File> noticeFiles) {
        int issueCount = 0;
        Set<String> shadingModules = new HashSet<>(modulesWithShadedDependencies.keys());
        shadingModules.removeAll(noticeFiles.stream().map(LicenseChecker::getModuleFromNoticeFile).collect(Collectors.toList()));
        for (String moduleWithoutNotice : shadingModules) {
            LOG.warn("Module {} is missing a NOTICE file. It has shaded dependencies.", moduleWithoutNotice);
            issueCount++;
        }
        return issueCount;
    }

    private static String getModuleFromNoticeFile(File noticeFile) {
        File moduleFile = noticeFile.getParentFile() // META-INF
            .getParentFile() // resources
            .getParentFile() // main
            .getParentFile() // src
            .getParentFile(); // <-- module name
        return moduleFile.getName();
    }

    private int checkNoticeFile(Multimap<String, Dependency> modulesWithShadedDependencies, File noticeFile) throws IOException {
        int issueCount = 0;
        String moduleName = getModuleFromNoticeFile(noticeFile);

        // 1st line contains module name
        String noticeContents = readFile(noticeFile.toPath());
        if (!noticeContents.startsWith(moduleName)) {
            String firstLine = noticeContents.substring(0, noticeContents.indexOf('\n'));
            LOG.warn("Expected first file of notice file to start with module name. moduleName={}, firstLine={}", moduleName, firstLine);
            issueCount++;
        }

        // collect all declared dependencies from NOTICE file
        Set<Dependency> declaredDependencies = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new StringReader(noticeContents))) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher noticeDependencyMatcher = NOTICE_DEPENDENCY_PATTERN.matcher(line);
                if (noticeDependencyMatcher.find()) {
                    String groupId = noticeDependencyMatcher.group(1);
                    String artifactId = noticeDependencyMatcher.group(2);
                    String version = noticeDependencyMatcher.group(3);
                    Dependency toAdd = Dependency.create(groupId, artifactId, version);
                    if (!declaredDependencies.add(toAdd)) {
                        LOG.warn("Dependency {} has been declared twice in module {}", toAdd, moduleName);
                        issueCount++;
                    }
                }
            }
        }
        // print all dependencies missing from NOTICE file
        Set<Dependency> expectedDependencies = new HashSet<>(modulesWithShadedDependencies.get(moduleName));
        expectedDependencies.removeAll(declaredDependencies);
        for (Dependency missingDependency : expectedDependencies) {
            LOG.warn("Could not find dependency {} in NOTICE file {}", missingDependency, noticeFile);
            issueCount++;
        }

        // print all dependencies defined in NOTICE file, which were not expected
        Set<Dependency> excessDependencies = new HashSet<>(declaredDependencies);
        excessDependencies.removeAll(modulesWithShadedDependencies.get(moduleName));
        for (Dependency excessDependency : excessDependencies) {
            LOG.warn("Dependency {} is mentioned in NOTICE file {}, but is not expected there", excessDependency, noticeFile);
            issueCount++;
        }

      /*  // all shaded dependencies are mentioned in the file
        Collection<Dependency> moduleDependencies = modulesWithShadedDependencies.get(moduleName);
        for (Dependency moduleDependency: moduleDependencies) {
            String expectedDependency = moduleDependency.toString();
            if (!noticeContents.contains(expectedDependency)) {
                LOG.warn("Could not find dependency {} in NOTICE file {}", expectedDependency, noticeFile);
                issueCount++;
            }
        } */

        // number of dependencies in NOTICE file == number of dependencies from build output

        return issueCount;
    }

    private static String readFile(Path path) throws IOException {
        byte[] encoded = Files.readAllBytes(path);
        return new String(encoded, Charset.defaultCharset());
    }

    private List<File> findNoticeFiles(File root) throws IOException {
        return Files.walk(root.toPath())
            .filter(file -> {
                int nameCount = file.getNameCount();
                return file.getName(nameCount - 1).toString().equals("NOTICE")
                    && file.getName(nameCount - 2).toString().equals("META-INF")
                    && file.getName(nameCount - 3).toString().equals("resources");
            })
            .map(Path::toFile)
            .collect(Collectors.toList());
    }

    private Multimap<String, Dependency> parseModulesFromBuildResult(File buildResult) throws IOException {
        Multimap<String, Dependency> result = ArrayListMultimap.create();
        try (BufferedReader br = new BufferedReader(new FileReader(buildResult))) {
            String line;
            String currentModule = null;
            while ((line = br.readLine()) != null) {
                Matcher nextModuleMatcher = NEXT_MODULE_PATTERN.matcher(line);
                if (nextModuleMatcher.find()) {
                    currentModule = nextModuleMatcher.group(1);
                }

                Matcher includeMatcher = INCLUDE_MODULE_PATTERN.matcher(line);
                if (includeMatcher.find()) {
                    String groupId = includeMatcher.group(1);
                    String artifactId = includeMatcher.group(2);
                    String version = includeMatcher.group(3);
                    if (!"org.apache.flink".equals(groupId)) {
                        result.put(currentModule, Dependency.create(groupId, artifactId, version));
                    }
                }
            }
        }
        return result;
    }

    private static class Dependency {

        private final String groupId;
        private final String artifactId;
        private final String version;

        public Dependency(String groupId, String artifactId, String version) {
            this.groupId = Preconditions.checkNotNull(groupId);
            this.artifactId = Preconditions.checkNotNull(artifactId);
            this.version = Preconditions.checkNotNull(version);
        }

        public static Dependency create(String groupId, String artifactId, String version) {
            return new Dependency(groupId, artifactId, version);
        }

        @Override
        public String toString() {
            return groupId + ":" + artifactId + ":" + version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Dependency that = (Dependency) o;

            if (!groupId.equals(that.groupId)) return false;
            if (!artifactId.equals(that.artifactId)) return false;
            return version.equals(that.version);
        }

        @Override
        public int hashCode() {
            int result = groupId.hashCode();
            result = 31 * result + artifactId.hashCode();
            result = 31 * result + version.hashCode();
            return result;
        }
    }
}
