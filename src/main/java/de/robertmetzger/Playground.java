package de.robertmetzger;




public class Playground {
 /*   public static void main(String[] args) throws DependencyResolutionException {
        File local = new File("/tmp/local-repository");
        Collection<RemoteRepository> remotes = Arrays.asList(
            new RemoteRepository(
                "maven-central",
                "default",
                "https://repo1.maven.org/maven2/"
            )
        );

        Collection<Artifact> deps = new Aether(remotes, local).resolve(
            new DefaultArtifact("org.apache.flink", "flink-core", "", "jar", "1.10.2"),
            "runtime"
        );


        System.out.println("Props = " + deps.iterator().next().getProperties());
        System.out.println("Type = " + deps.iterator().next().getClass());
    } */

    // src https://stackoverflow.com/questions/39638138/find-all-direct-dependencies-of-an-artifact-on-maven-central/39641359#39641359
  /*  public static void main(final String[] args) throws Exception {
        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
        RepositorySystem system = newRepositorySystem(locator);
        RepositorySystemSession session = newSession(system);

        RemoteRepository central = new RemoteRepository.Builder("central", "default", "http://repo1.maven.org/maven2/").build();

        Artifact artifact = new DefaultArtifact("org.apache.flink:flink-core:1.11.1");
        ArtifactDescriptorRequest request = new ArtifactDescriptorRequest(artifact, Arrays.asList(central), null);
        ArtifactDescriptorResult result = system.readArtifactDescriptor(session, request);

        for (Dependency dependency : result.getDependencies()) {
            System.out.println(dependency);
        }
    }

    private static RepositorySystem newRepositorySystem(DefaultServiceLocator locator) {
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
       // locator.addService(TransporterFactory.class, FileTransporterFactory.class);
       // locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
        locator.addService(TransporterFactory.class, WagonTransporterFactory.class);
        return locator.getService(RepositorySystem.class);
    }

    private static RepositorySystemSession newSession(RepositorySystem system) {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
        LocalRepository localRepo = new LocalRepository("target/local-repo");
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));
        // set possible proxies and mirrors
        //session.setProxySelector(new DefaultProxySelector().add(new Proxy(Proxy.TYPE_HTTP, "host", 3625), Arrays.asList("localhost", "127.0.0.1")));
        session.setMirrorSelector(new DefaultMirrorSelector().add("my-mirror", "http://mirror", "default", false, "external:*", null));
        return session;
    } */
}
