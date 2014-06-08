package de.robertmetzger.github2jira;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import net.rcarz.jiraclient.BasicCredentials;
import net.rcarz.jiraclient.Field;
import net.rcarz.jiraclient.Issue.FluentCreate;
import net.rcarz.jiraclient.Issue.SearchResult;
import net.rcarz.jiraclient.JiraClient;
import net.rcarz.jiraclient.JiraException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.eclipse.egit.github.core.Comment;
import org.eclipse.egit.github.core.Issue;
import org.eclipse.egit.github.core.Label;
import org.eclipse.egit.github.core.User;
import org.eclipse.egit.github.core.client.PageIterator;
import org.eclipse.egit.github.core.service.IssueService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Hello world!
 *
 */
public class App  {
	public static final String nl = "\n";
	
	
    public static void main( String[] args ) throws IOException, JiraException  {
    	
    	Properties prop = new Properties();
    	InputStream input = null;
     
    	try {
     
    		input = new FileInputStream("config.properties");
    		// load a properties file
    		prop.load(input);
    	} catch(Exception e) {
    		System.err.println("Error loading 'conf.properties'");
    		e.printStackTrace();
    		System.exit(1);
    	}
    		
    	final String ghUser = prop.getProperty("github.user");
    	final String ghRepo = prop.getProperty("github.repository");
    	final String jiraProject = prop.getProperty("jira.project");
    	
    	
    	BasicCredentials creds = new BasicCredentials(prop.getProperty("jira.username"), prop.getProperty("jira.password"));
    	JiraClient jc = new JiraClient("https://issues.apache.org/jira", creds);
    	
    	IssueService is = new IssueService();
    	PageIterator<Issue> issuesPager = is.pageIssues(ghUser, ghRepo, ImmutableMap.of("direction", "asc", "state", "all", "filter", "all"), 1, 1);
    	int c = 0;
    	while(issuesPager.hasNext()) {
    		Collection<Issue> issues = issuesPager.next();
	    	for(Issue i : issues) {
	    		System.err.println("#"+i.getNumber()+" body: "+i.getTitle());
	    		
	    		String issueType = "Improvement";
	    		if(isBug(i.getLabels())) {
	    			issueType = "Bug";
	    		}
	    		FluentCreate fluent = jc.createIssue(jiraProject, issueType);
	    		
	    		String importInformation = "---------------- Imported from GitHub ----------------"+nl;
	    		importInformation += "Url: "+i.getHtmlUrl()+nl;
	    		importInformation += "Created by: "+userToUrl(i.getUser())+nl;
	    		importInformation += "Labels: "+ghLabelsToString(i)+nl;
	    		if(i.getMilestone() != null) {
	    			importInformation += "Milestone: "+i.getMilestone().getTitle()+nl;
	    		}
	    		if(i.getAssignee() != null) {
	    			importInformation += "Assignee: "+userToUrl(i.getAssignee())+nl;
	    		}
	    		importInformation += "Created at: "+i.getCreatedAt()+nl;
	    		importInformation += "State: "+i.getState()+nl; 
	    		fluent.field(Field.DESCRIPTION, i.getBody()+nl+nl+importInformation);
	    		fluent.field(Field.SUMMARY, "[GitHub] "+i.getTitle());
	    		fluent.field(Field.LABELS, ImmutableSet.of("github import"));
	    		fluent.field(Field.FIX_VERSIONS, ImmutableSet.of("pre-apache"));
	    	//	fluent.field(Field.STATUS, "");
	    		net.rcarz.jiraclient.Issue jiraIssue = fluent.execute();
	    		String diffUrl = i.getPullRequest().getDiffUrl();
	    		if(diffUrl != null && diffUrl.length() > 0) {
	    			// issue is pull request
	    			
	    		}
	    		List<Comment> ghComments = is.getComments(ghUser, ghRepo, i.getNumber());
	    		for(Comment com : ghComments) {
	    			jiraIssue.addComment("[GitHub Import] [Date: "+com.getCreatedAt()+", Author: "+userToUrl(com.getUser())+"]"+nl+
	    					com.getBody());
	    		}
	    		if(i.getState().equals("closed")) {
	    			jiraIssue.transition().execute("Closed");
	    		}
	    		
	    		
	    		//if( c++ > 10) {
	    			System.exit(1);
	    		// }
	    	}
    	}
//    	
    	
    	
    }
    
    private static String userToUrl(User user) {
		return "["+user.getLogin()+"|"+user.getHtmlUrl()+"]";
	}

	private static String ghLabelsToString(Issue i) {
		String ret = "";
		for(Label l: i.getLabels()) {
			ret += l.getName()+", ";
		}
		return ret;
	}

	/**
     * Very simple policy:
     * Contains "bug" => isBug
     * contains "enhancement" => noBug.
     * 
     * If both => isBug
     */
    private static boolean isBug(List<Label> labels) {
    	for(Label l : labels) {
    		if(l.getName().equals("bug")) {
    			return true;
    		}
    		if(l.getName().equals("enhancement")) {
    			return false;
    		}
    	}
    	return true;
	}
}

	