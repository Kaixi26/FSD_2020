# Example test usage

Compile code:

`mvn compile`

Start servers:

`mvn exec:java -q -Dexec.mainClass="Main" -Dexec.args="10000 10010 0 localhost:10011 1 localhost:10012 2"`

`mvn exec:java -q -Dexec.mainClass="Main" -Dexec.args="10001 10011 1 localhost:10010 0 localhost:10012 2"`

`mvn exec:java -q -Dexec.mainClass="Main" -Dexec.args="10002 10012 2 localhost:10011 1 localhost:10010 0"`

Run test:

`mvn exec:java -q -Dexec.mainClass="Test.ClientGet" -Dexec.args="10 20 100"`
