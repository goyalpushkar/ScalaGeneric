package com.practice.bagsQueuesStacks;

/*
 * Write a filter InfixToPostfix that converts an arithmetic expression from infix to postfix.
 */
/* 
 * Solution hint
 * Scan the infix expression from left to right.
	Operand: output it.
	Left parentheses: push onto stack.
	Right parentheses: repeatedly pop elements from the stack and output them until a left parenthesis is encountered. Discard both parentheses.
	Operator with high precedence than top of stack: push onto stack.
	Operator with lower or equal precedence than top of stack: repeatedly pop elements from the stack and output them until top of stack has higher precedence. Push the scanned operator onto the stack.
	Afterward, pop remaining elements off stack and output them.
 */

public class ExerInfixToPostfix {

}
