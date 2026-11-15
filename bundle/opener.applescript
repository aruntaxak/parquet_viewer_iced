-- Parquet Opener: helper droplet that owns the .parquet file association.
--
-- Why it exists: the main viewer is an eframe/winit app, and eframe 0.28 only
-- invokes the app-creator closure lazily on the first Event::Resumed -- after
-- NSApplication has already dispatched the cold-launch odoc AppleEvent to
-- winit's own delegate (which drops it). So double-clicking a file with the
-- app closed can never reach the viewer directly. This droplet receives the
-- odoc (AppleScript applets handle it natively, before any event loop runs)
-- and forwards each file as argv, which the viewer handles in main().
on run
	-- Launched directly with no files: open the viewer (shows file dialog).
	launchViewer({})
end run

on open theFiles
	set posixFiles to {}
	repeat with f in theFiles
		set end of posixFiles to POSIX path of f
	end repeat
	launchViewer(posixFiles)
end open

on launchViewer(posixFiles)
	-- Hand off to launch.sh (keeps quoting sane -- no embedded quotes here).
	set launcherPosix to (POSIX path of (path to me)) & "Contents/Resources/launch.sh"
	set shellCmd to quoted form of launcherPosix
	repeat with pf in posixFiles
		set shellCmd to shellCmd & " " & quoted form of pf
	end repeat
	try
		do shell script shellCmd
	end try
end launchViewer
