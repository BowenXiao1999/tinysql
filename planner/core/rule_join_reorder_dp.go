// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
)

type joinReorderDPSolver struct {
	*baseSingleGroupJoinOrderSolver
	newJoin func(lChild, rChild LogicalPlan, eqConds []*expression.ScalarFunction, otherConds []expression.Expression) LogicalPlan
}

type joinGroupEqEdge struct {
	nodeIDs []int
	edge    *expression.ScalarFunction
}

type joinGroupNonEqEdge struct {
	nodeIDs    []int
	nodeIDMask uint
	expr       expression.Expression
}

func (s *joinReorderDPSolver) solve(joinGroup []LogicalPlan, eqConds []expression.Expression) (LogicalPlan, error) {
	// TODO: You need to implement the join reorder algo based on DP.

	// The pseudo code can be found in README.
	// And there's some common struct and method like `baseNodeCumCost`, `calcJoinCumCost` you can use in `rule_join_reorder.go`.
	// Also, you can take a look at `rule_join_reorder_greedy.go`, this file implement the join reorder algo based on greedy algorithm.
	// You'll see some common usages in the greedy version.

	// Note that the join tree may be disconnected. i.e. You need to consider the case `select * from t, t1, t2`.

	// for _, node := range joinGroup {
	// 	_, err := node.recursiveDeriveStats()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	s.curJoinGroup = append(s.curJoinGroup, &jrNode{
	// 		p:       node,
	// 		cumCost: s.baseNodeCumCost(node),
	// 	})
	// }
	// adjacents := make([][]int, len(s.curJoinGroup))
	// totalEqEdges := make([]joinGroupEqEdge, 0, len(eqConds))
	// addEqEdge := func(node1, node2 int, edgeContent *expression.ScalarFunction) {
	// 	totalEqEdges = append(totalEqEdges, joinGroupEqEdge{
	// 		nodeIDs: []int{node1, node2},
	// 		edge:    edgeContent,
	// 	})
	// 	adjacents[node1] = append(adjacents[node1], node2)
	// 	adjacents[node2] = append(adjacents[node2], node1)
	// }
	// // Build Graph for join group
	// for _, cond := range eqConds {
	// 	sf := cond.(*expression.ScalarFunction)
	// 	lCol := sf.GetArgs()[0].(*expression.Column)
	// 	rCol := sf.GetArgs()[1].(*expression.Column)
	// 	lIdx, err := findNodeIndexInGroup(joinGroup, lCol)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	rIdx, err := findNodeIndexInGroup(joinGroup, rCol)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	addEqEdge(lIdx, rIdx, sf)
	// }
	// totalNonEqEdges := make([]joinGroupNonEqEdge, 0, len(s.otherConds))
	// for _, cond := range s.otherConds {
	// 	cols := expression.ExtractColumns(cond)
	// 	mask := uint(0)
	// 	ids := make([]int, 0, len(cols))
	// 	for _, col := range cols {
	// 		idx, err := findNodeIndexInGroup(joinGroup, col)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		ids = append(ids, idx)
	// 		mask |= 1 << uint(idx)
	// 	}
	// 	totalNonEqEdges = append(totalNonEqEdges, joinGroupNonEqEdge{
	// 		nodeIDs:    ids,
	// 		nodeIDMask: mask,
	// 		expr:       cond,
	// 	})
	// }
	// visited := make([]bool, len(joinGroup))
	// nodeID2VisitID := make([]int, len(joinGroup))
	// var joins []LogicalPlan
	// // BFS the tree.
	// for i := 0; i < len(joinGroup); i++ {
	// 	if visited[i] {
	// 		continue
	// 	}
	// 	visitID2NodeID := s.bfsGraph(i, visited, adjacents, nodeID2VisitID)
	// 	nodeIDMask := uint(0)
	// 	for _, nodeID := range visitID2NodeID {
	// 		nodeIDMask |= 1 << uint(nodeID)
	// 	}
	// 	var subNonEqEdges []joinGroupNonEqEdge
	// 	for i := len(totalNonEqEdges) - 1; i >= 0; i-- {
	// 		// If this edge is not the subset of the current sub graph.
	// 		if totalNonEqEdges[i].nodeIDMask&nodeIDMask != totalNonEqEdges[i].nodeIDMask {
	// 			continue
	// 		}
	// 		newMask := uint(0)
	// 		for _, nodeID := range totalNonEqEdges[i].nodeIDs {
	// 			newMask |= 1 << uint(nodeID2VisitID[nodeID])
	// 		}
	// 		totalNonEqEdges[i].nodeIDMask = newMask
	// 		subNonEqEdges = append(subNonEqEdges, totalNonEqEdges[i])
	// 		totalNonEqEdges = append(totalNonEqEdges[:i], totalNonEqEdges[i+1:]...)
	// 	}
	// 	// Do DP on each sub graph.
	// 	join, err := s.dpGraph(visitID2NodeID, nodeID2VisitID, joinGroup, totalEqEdges, subNonEqEdges)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	joins = append(joins, join)
	// }
	// remainedOtherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	// for _, edge := range totalNonEqEdges {
	// 	remainedOtherConds = append(remainedOtherConds, edge.expr)
	// }
	// // Build bushy tree for cartesian joins.
	// return s.makeBushyJoin(joins, remainedOtherConds), nil
	return nil, errors.Errorf("unimplemented")
}

func (s *joinReorderDPSolver) newJoinWithEdge(leftPlan, rightPlan LogicalPlan, edges []joinGroupEqEdge, otherConds []expression.Expression) (LogicalPlan, error) {
	var eqConds []*expression.ScalarFunction
	for _, edge := range edges {
		lCol := edge.edge.GetArgs()[0].(*expression.Column)
		rCol := edge.edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) {
			eqConds = append(eqConds, edge.edge)
		} else {
			newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
			eqConds = append(eqConds, newSf)
		}
	}
	join := s.newJoin(leftPlan, rightPlan, eqConds, otherConds)
	_, err := join.recursiveDeriveStats()
	return join, err
}

// Make cartesian join as bushy tree.
func (s *joinReorderDPSolver) makeBushyJoin(cartesianJoinGroup []LogicalPlan, otherConds []expression.Expression) LogicalPlan {
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup := make([]LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			// TODO:Since the other condition may involve more than two tables, e.g. t1.a = t2.b+t3.c.
			//  So We'll need a extra stage to deal with it.
			// Currently, we just add it when building cartesianJoinGroup.
			mergedSchema := expression.MergeSchema(cartesianJoinGroup[i].Schema(), cartesianJoinGroup[i+1].Schema())
			var usedOtherConds []expression.Expression
			otherConds, usedOtherConds = expression.FilterOutInPlace(otherConds, func(expr expression.Expression) bool {
				return expression.ExprFromSchema(expr, mergedSchema)
			})
			resultJoinGroup = append(resultJoinGroup, s.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1], nil, usedOtherConds))
		}
		cartesianJoinGroup = resultJoinGroup
	}
	return cartesianJoinGroup[0]
}

func findNodeIndexInGroup(group []LogicalPlan, col *expression.Column) (int, error) {
	for i, plan := range group {
		if plan.Schema().Contains(col) {
			return i, nil
		}
	}
	return -1, ErrUnknownColumn.GenWithStackByArgs(col, "JOIN REORDER RULE")
}
