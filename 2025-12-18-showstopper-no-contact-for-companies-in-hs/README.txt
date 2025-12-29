The issue turned-out to be unrelated to the analysis performed here.

It was emails containing special unicode characters (zero-width spaces, control chars, etc.) that faild HS's email validation.
A single contact with a bad email was enough to fail the entire batch of contacts, which caused a lot of contacts to be missing.