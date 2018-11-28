package Server.Common;

public enum Status {
	None,
	PrepareStarted,
	PersistentMemoryWritten,
	DecidedAbort,
	WaitingForResponse,
	AbortReceived,
	CommitReceived
};

