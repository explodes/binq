package main

import (
	"explodes/github.com/binq"
	"fmt"
	"strings"
)

const sampleFilter = `
# This is a comment.
 # Key's first u64le is equal to 7
(KEY(0, U64LE) = U64(7)
	OR  # either case is fine.
	# Key's second u64le is greater than or equal to 9	
	KEY(8, U64LE) >= U64(9))
AND
# The u32le at the address pointed to by the first u16le in value does not equal 100
VALUE(JUMP(0, U16LE), U32LE) != U32(10)
AND 
false != true OR "abc" < "123"
`

func main() {
	fmt.Print(sampleFilter)

	p := binq.NewParser(sampleFilter)

	lines := strings.Split(sampleFilter, "\n")

	values, err := p.ReadUnsupportedValues()
	if err != nil {
		fmt.Println(err)
	}
	var functionalValues []*binq.ParserValue
	for _, pv := range values {
		if pv.Token().IsIgnored() || pv.Token().IsParenthesis() {
			continue
		}
		functionalValues = append(functionalValues, pv)
	}
	fmt.Println("FUNCTIONAL VALUES", len(functionalValues))
	for _, value := range functionalValues {
		simplePrint(value)
	}
	fmt.Println()

	values, err = p.ToPostfix(values)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("POSTFIX VALUES", len(values))
	for _, value := range values {
		simplePrint(value)
	}
	fmt.Println()

	fmt.Println("RESULTS", len(values))
	for _, value := range values {
		verbosePrint(value, lines)
	}

}

func verbosePrint(value *binq.ParserValue, lines []string) {
	fmt.Printf("%02d:%02d :: %s: %s\n", value.Line(), value.LinePos(), value.Token(), value.Value())
	fmt.Printf("    |%s\n", lines[value.Line()])
	fmt.Printf("    |%s^\n", strings.Repeat(" ", value.LinePos()))
}

func simplePrint(value *binq.ParserValue) {
	strVal := strings.Trim(value.Value(), " \n\r\b")
	fmt.Printf("%02d:%02d :: %s: %s\n", value.Line(), value.LinePos(), value.Token(), strVal)
}
