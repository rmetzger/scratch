package de.robertmetzger.github2jira;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import net.rcarz.jiraclient.BasicCredentials;
import net.rcarz.jiraclient.Field;
import net.rcarz.jiraclient.Issue.FluentCreate;
import net.rcarz.jiraclient.JiraClient;
import net.rcarz.jiraclient.JiraException;

import org.apache.commons.io.FileUtils;
import org.eclipse.egit.github.core.Comment;
import org.eclipse.egit.github.core.Issue;
import org.eclipse.egit.github.core.Label;
import org.eclipse.egit.github.core.User;
import org.eclipse.egit.github.core.client.GitHubClient;
import org.eclipse.egit.github.core.client.PageIterator;
import org.eclipse.egit.github.core.service.IssueService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Import GitHub issues into JIRA.
 * 
 * @author Robert Metzger (rmetzger@apache.org)
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
    	int startAt = Integer.valueOf(prop.getProperty("github.startAt"));
    	
    	
    	BasicCredentials creds = new BasicCredentials(prop.getProperty("jira.username"), prop.getProperty("jira.password"));
    	JiraClient jc = new JiraClient(prop.getProperty("jira.url"), creds);
    	
    	GitHubClient ghClient = new GitHubClient();
    	ghClient.setCredentials(prop.getProperty("github.importuser"), prop.getProperty("github.importpassword"));
    	IssueService is = new IssueService(ghClient);
    	
    	
    	PageIterator<Issue> issuesPager = is.pageIssues(ghUser, ghRepo, ImmutableMap.of("direction", "asc", "state", "all", "filter", "all"), startAt, 1);
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
	    		fluent.field(Field.DESCRIPTION, autorefIssuesInText(i.getBody(), prop)+nl+nl+importInformation);
	    		fluent.field(Field.SUMMARY, i.getTitle());
	    		fluent.field(Field.LABELS, ImmutableSet.of("github-import"));
	    		fluent.field(Field.FIX_VERSIONS, ImmutableSet.of("pre-apache"));
	    		
	    		net.rcarz.jiraclient.Issue jiraIssue = fluent.execute();
	    		String patchURL = i.getPullRequest().getPatchUrl();
	    		if(patchURL != null && patchURL.length() > 0) {
	    			// issue is pull request
	    			File patch = File.createTempFile("pull-request-"+i.getNumber()+"-", ".patch");
	    			FileUtils.copyURLToFile(new URL(patchURL), patch);
	    			if(patch.length() >= 10485760) {
	    				System.err.println("This issue's attachment is larger than 10 MB.");
	    				jiraIssue.addComment("Unable to add patch as an attachment, since its larger than 10 MB");
	    			} else {
	    				jiraIssue.addAttachment(patch);
	    			}
	    		}
	    		List<Comment> ghComments = is.getComments(ghUser, ghRepo, i.getNumber());
	    		for(Comment com : ghComments) {
	    			jiraIssue.addComment("[Date: "+com.getCreatedAt()+", Author: "+userToUrl(com.getUser())+"]"+nl+nl+
	    					autorefIssuesInText(com.getBody(), prop) );
	    		}
	    		if(i.getState().equals("closed")) {
	    			jiraIssue.transition().execute("Close Issue");
	    		}
	    		
	    		System.err.println("Import "+c+ " done. Remaining requests "+ghClient.getRemainingRequests());
	    	}
    	}
    	
    }
    

    		
    public static String autorefIssuesInText(String text, Properties props) {
    	// github url 2 jira url
    	text = text.replaceAll("\\[([^\\]]+)\\]\\(([^\\]]+)\\)", "[$1|$2]");
    	
    	// issue id to jira / github link
    	text = text.replaceAll("\\#([0-9]+)", 
    		"([\\#$1|https://github.com/"+props.getProperty("github.user")+"/"+props.getProperty("github.repository")+"/issues/$1]"
    				+ " | "
    				+ "[FLINK-$1|"+props.getProperty("jira.url")+"/browse/"+props.getProperty("jira.project")+"-$1])");
    	// commit hash to github link
    	text = text.replaceAll("([^/]{1})([a-z0-9]{40})", "$1[$2|https://github.com/"+props.getProperty("github.user")+"/"+props.getProperty("github.repository")+"/commit/$2]");
    	
    	return text;
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

	