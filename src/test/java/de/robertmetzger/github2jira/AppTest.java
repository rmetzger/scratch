package de.robertmetzger.github2jira;

import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;

public class AppTest {
	
	
	@Test
	public void testParser() {
		System.err.println("Test");
		Properties p = new Properties();
		p.setProperty("github.user", "stratosphere");
		p.setProperty("github.repository", "stratosphere");
		p.setProperty("jira.url", "https://issues.apache.org/jira");
		p.setProperty("jira.project", "FLINK");
		
		Assert.assertEquals("Shouldn't we close this now? Seems to be resolved by ([#238|https://github.com/stratosphere/stratosphere/issues/238] | [FLINK-238|https://issues.apache.org/jira/browse/FLINK-238]).", 
				App.autorefIssuesInText("Shouldn't we close this now? Seems to be resolved by #238.",p) );
		
		Assert.assertEquals("Fixed by ([#16|https://github.com/stratosphere/stratosphere/issues/16] | [FLINK-16|https://issues.apache.org/jira/browse/FLINK-16])", 
				App.autorefIssuesInText("Fixed by #16",p) );
		
		Assert.assertEquals("Fixed in [7671b33b1b4a21b05d6ce5cbb878cc1000fe2cc3|https://github.com/stratosphere/stratosphere/commit/7671b33b1b4a21b05d6ce5cbb878cc1000fe2cc3]\n", 
				App.autorefIssuesInText("Fixed in 7671b33b1b4a21b05d6ce5cbb878cc1000fe2cc3\n",p) );
		
		Assert.assertEquals("[TestBase2|https://github.com/stratosphere/stratosphere/blob/bdd378c8b754d05a32ad260ada6843c104ffbf2a/stratosphere-tests/src/main/java/eu/stratosphere/test/util/TestBase2.java] has been moved from the tests folder of `stratosphere-tests` to the main folder with the renaming and is available to the outside. Is this enough?", 
				App.autorefIssuesInText("[TestBase2](https://github.com/stratosphere/stratosphere/blob/bdd378c8b754d05a32ad260ada6843c104ffbf2a/stratosphere-tests/src/main/java/eu/stratosphere/test/util/TestBase2.java) has been "
						+ "moved from the tests folder of `stratosphere-tests` to the main folder with the renaming and is available to the outside. Is this enough?",p) );
		
	}
}
