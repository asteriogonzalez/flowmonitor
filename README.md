# flowmonitor
This module allows you to monitorize the file system looking for changes and fires actions based on user handlers that are attached to the flow core.

### Example:

  $ flowmonitor.py pelican . pytest . pelican ~/Documents/blog


Will monitorize . with a pelican handler and pytest handler and ~/Documents/blog with another pelican handler as well.
