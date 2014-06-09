# GitHub to JIRA import tool

The tool is using the following libraries:
- "jira-client": https://github.com/rcarz/jira-client (LGPL)
- "GitHub Java API (org.eclipse.egit.github.core)": https://github.com/eclipse/egit-github/tree/master/org.eclipse.egit.github.core (EPL - http://www.eclipse.org/legal/epl-v10.html)


My code is licensed under the Apache Software License 2.0.
This work is private and has nothing to do with the Apache Software foundation.


Limitations:
- No conversion of Markdown into JIRA syntax (except for links)
- The code assumes that the GitHub issue ID = JIRA issue id (so you basically have to do the import against an empty JIRA project)
- maybe more ;)

Use the "Issues" here to send me feedback or questions.
