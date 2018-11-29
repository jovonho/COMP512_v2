package Server.Common;

public enum TwoPCstatus
{
	None,
	PrepareStarted,
	SentPrep1,
	SentPrep2,
	SentPrep3,
	SentPrep4,
	gotAllVotes,
	Committing,
	DecidedAbort,
	WaitingForResponse,
	AbortReceived,
	CommitReceived
}
