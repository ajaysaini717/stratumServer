// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract MultiSend {
    address public owner;

    modifier onlyOwner() {
        require(msg.sender == owner, "MultiSend: caller is not owner");
        _;
    }

    constructor() {
        owner = msg.sender;
    }

    receive() external payable {}

    function multiSend(
        address[] calldata recipients,
        uint256[] calldata amounts
    ) external onlyOwner {
        require(
            recipients.length == amounts.length,
            "MultiSend: length mismatch"
        );
        for (uint256 i = 0; i < recipients.length; i++) {
            payable(recipients[i]).transfer(amounts[i]);
        }
    }
}