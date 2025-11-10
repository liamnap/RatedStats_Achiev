local RatedStats, Achiev = ...
local playerName   = UnitName("player") .. "-" .. GetRealmName()

local f = CreateFrame("Frame")
f:RegisterEvent("ADDON_LOADED")
f:SetScript("OnEvent", function(self, event, addonName)
    if addonName ~= "RatedStats" then return end

    -- now it's safe to grab the namespace
    local RSTATS = _G.RSTATS

    if RSTATS then
        RSTATS:RegisterModule("Achiev", "Achievement Tooltip Enhancements", true)
    end

    -- we don't need this frame any more
    self:UnregisterEvent("ADDON_LOADED")
end)

-- Determine the region and use the appropriate achievement data
local regionMap = {
    [1] = "US",
    [2] = "KR",
    [3] = "EU",
    [4] = "TW",
}

local regionID = GetCurrentRegion()
local regionCode = regionMap[regionID] or "US"

-- Merge monolithic or chunked achievement files into one table
local function mergeRegionParts(region)
    local merged = {}
    local baseName = "ACHIEVEMENTS_" .. region
    local base = _G[baseName]
    if type(base) == "table" then
        for _, v in ipairs(base) do
            table.insert(merged, v)
        end
    end

    local partIndex = 1
    while true do
        local part = _G[baseName .. "_PART" .. partIndex]
        if type(part) ~= "table" then break end
        for _, v in ipairs(part) do
            table.insert(merged, v)
        end
        partIndex = partIndex + 1
    end

    return merged
end

local regionData = mergeRegionParts(regionCode)

-- Cache table to avoid repeat lookups
local achievementCache = {}

local PvpRankColumns = {
    { key = "a", label = "Co-I",   prefix = "Combatant I" },
    { key = "b", label = "Co-II",  prefix = "Combatant II" },
    { key = "c", label = "Ch-I",   prefix = "Challenger I" },
    { key = "d", label = "Ch-II",  prefix = "Challenger II" },
    { key = "e", label = "R-I",    prefix = "Rival I" },
    { key = "f", label = "R-II",   prefix = "Rival II" },
    { key = "g", label = "Duel",   prefix = "Duelist" },
    { key = "h", label = "Elite",  prefix = "Elite" },
    { key = "i", label = "Glad",   prefix = "Gladiator" },
    { key = "j", label = "Legend", prefix = "Legend:" },
    { key = "k", label = "Rank 1",     r1 = true },
    { key = "l", label = "HotX",   hero = true },
}

local R1Titles = {
    "Primal Gladiator", "Wild Gladiator", "Warmongering Gladiator",
    "Vindictive Gladiator", "Fearless Gladiator", "Cruel Gladiator",
    "Ferocious Gladiator", "Fierce Gladiator", "Demonic Gladiator",
    "Dread Gladiator", "Sinister Gladiator", "Notorious Gladiator",
    "Corrupted Gladiator", "Sinful Gladiator", "Unchained Gladiator",
    "Cosmic Gladiator", "Eternal Gladiator", "Crimson Gladiator",
    "Obsidian Gladiator", "Draconic Gladiator", "Seasoned Gladiator",
    "Forged Warlord", "Forged Marshal", "Forged Legend", "Forged Gladiator",
    "Prized Warlord", "Prized Marshal", "Prized Legend", "Prized Gladiator"
}

local HeroTitles = {
    "Hero of the Horde",
    "Hero of the Alliance"
}

local PvpRankColumns = {
    { key = "a", prefix = "Combatant I",  icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_01_Small.blp" },
    { key = "b", prefix = "Combatant II", icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_02_Small.blp" },
    { key = "c", prefix = "Challenger I", icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_03_Small.blp" },
    { key = "d", prefix = "Challenger II",icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_04_Small.blp" },
    { key = "e", prefix = "Rival I",      icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_05_Small.blp" },
    { key = "f", prefix = "Rival II",     icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_06_Small.blp" },
    { key = "g", prefix = "Duelist",      icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_07_Small.blp" },
    { key = "h", prefix = "Gladiator",    icon = "Interface\\Icons\\Achievement_FeatsOfStrength_Gladiator_03.blp" },
    { key = "i", prefix = "Elite",        icon = "Interface\\Icons\\Achievement_FeatsOfStrength_Gladiator_07.blp" },
    { key = "j", prefix = "Legend:",      icon = "Interface\\Icons\\Achievement_FeatsOfStrength_Gladiator_08.blp" },
    { key = "k", r1 = true,               icon = "Interface\\Icons\\Achievement_FeatsOfStrength_Gladiator_08.blp" },
    { key = "l", hero = true, icons = {
        "Interface\\PvPRankBadges\\PvPRankHorde.blp",
        "Interface\\PvPRankBadges\\PvPRankAlliance.blp"
    } },
}

-- Center text in fixed-width column
local function centerText(text, width)
    local str = tostring(text)
    local len = #str
    local pad = math.max(0, math.floor((width - len) / 2))
    return string.rep(" ", pad) .. str .. string.rep(" ", width - len - pad)
end

local function GetPvpAchievementSummary(entry)
    local summary = {}
    local highestRank = nil
    local highestRankIndex = 0

    for _, col in ipairs(PvpRankColumns) do
        summary[col.key] = 0
    end

    for _, val in pairs(entry) do
        if type(val) == "string" then
            local name = val:lower()

            for i, col in ipairs(PvpRankColumns) do
                if col.prefix and name:find(col.prefix:lower(), 1, true) then
                    summary[col.key] = summary[col.key] + 1
                    if i > highestRankIndex then
                        highestRank = val  -- ✅ store actual match string
                        highestRankIndex = i
                    end
                elseif col.r1 then
                    for _, r1 in ipairs(R1Titles) do
                        if name:find(r1:lower(), 1, true) then
                            summary[col.key] = summary[col.key] + 1
                            if i > highestRankIndex then
                                highestRank = r1
                                highestRankIndex = i
                            end
                        end
                    end
                elseif col.hero then
                    for _, hero in ipairs(HeroTitles) do
                        if name:find(hero:lower(), 1, true) then
                            summary[col.key] = summary[col.key] + 1
                            if i > highestRankIndex then
                                highestRank = hero
                                highestRankIndex = i
                            end
                        end
                    end
                end
            end
        end
    end

    return { summary = summary, highest = highestRank }
end

local function centerIcon(iconTag, width)
    local len = 3 -- 3 visual units for the icon
    local pad = math.max(0, math.floor((width - len) / 2))
    return string.rep(" ", pad) .. iconTag .. string.rep(" ", width - len - pad)
end

local function AddAchievementInfoToTooltip(tooltip, overrideName, overrideRealm)
    -- look up our per-char database and bail out if Achiev is off
    local key = UnitName("player") .. "-" .. GetRealmName()
    local db  = RSTATS.Database[key]
	local module = "RatedStats_Achiev"
    if C_AddOns.GetAddOnEnableState(module, nil) == 0 then
        return
    end
  
    local baseName, realm

    if overrideName then
        baseName = overrideName
        realm = overrideRealm or GetRealmName()
    else
        local _, unit = tooltip:GetUnit()
        if not unit or not UnitIsPlayer(unit) then return end
        baseName, realm = UnitFullName(unit)
        if not baseName then return end
    end

    realm = realm or GetRealmName()
    local fullName = (baseName .. "-" .. realm:gsub("%s+", "")):lower()

    -- Cache lookup
    if achievementCache[fullName] == nil then
        local found = false
        for _, entry in ipairs(regionData) do
            if entry.character and entry.character:lower() == fullName then
                achievementCache[fullName] = GetPvpAchievementSummary(entry)
                found = true
                break
            end
        end
        if not found then
            achievementCache[fullName] = { summary = {}, highest = nil }
        end
    end

	local result = achievementCache[fullName]
	local summary = result.summary or {}
	local highest = result.highest

    tooltip:AddLine("|cffffff00Rated Stats - Achievements|r")
    tooltip:AddLine("----------------------------")

    if highest then
        tooltip:AddLine("|cff00ff00Highest PvP Rank:|r " .. highest)
    else
        tooltip:AddLine("|cffff0000No History / Not Seen in Bracket|r")
    end

    local hasAnyHistory = false
    for _, col in ipairs(PvpRankColumns) do
        if summary[col.key] and summary[col.key] > 0 then
            hasAnyHistory = true
            break
        end
    end
	
	if hasAnyHistory then
		local iconRow, valueRow = "", ""
		local iconSize = 16
		local iconOffsetY = 0
	
		for _, col in ipairs(PvpRankColumns) do
			local count = summary[col.key] or 0
	
			-- Handle HotX (double icon column)
			if col.hero and col.icons then
				local icons = ""
				for _, iconPath in ipairs(col.icons) do
					icons = icons .. string.format("|T%s:%d:%d:0:%d|t", iconPath, iconSize, iconSize, iconOffsetY)
				end
				iconRow = iconRow .. centerIcon(icons, 10)
				valueRow = valueRow .. centerText(count, 12)
	
			else
				local iconTag = string.format("|T%s:%d:%d:0:%d|t", col.icon or "Interface\\Icons\\inv_misc_questionmark", iconSize, iconSize, iconOffsetY)
				iconRow = iconRow .. centerIcon(iconTag, 6)
				valueRow = valueRow .. centerText(count, 6)
			end
		end
	
		tooltip:AddLine(iconRow)
		tooltip:AddLine(valueRow)
	end

    tooltip:Show()
end

-- Defer hook until player is fully in the game
local f = CreateFrame("Frame")
f:RegisterEvent("PLAYER_LOGIN")
f:RegisterEvent("UPDATE_MOUSEOVER_UNIT") 

f:SetScript("OnEvent", function(_, event)
    if event == "PLAYER_LOGIN" then
        if GameTooltip:HasScript("OnTooltipSetUnit") then
            GameTooltip:HookScript("OnTooltipSetUnit", AddAchievementInfoToTooltip)
        end
        hooksecurefunc(GameTooltip, "SetUnit", AddAchievementInfoToTooltip)

        -- Hook LFG tooltips
        hooksecurefunc("LFGListUtil_SetSearchEntryTooltip", function(tooltip, resultID)
            local id, activityID, name, comment, voiceChat, iLvl, age, numBNetFriends, numCharFriends, numGuildMates, isDelisted, leaderName = C_LFGList.GetSearchResultInfo(resultID)
            if leaderName then
                local realm = GetNormalizedRealmName() or GetRealmName()
                tooltip:SetOwner(UIParent, "ANCHOR_CURSOR")
                tooltip:SetText(name)
                -- Simulate a unit structure
                AddAchievementInfoToTooltip({
                    GetUnit = function() return nil, nil end,
                    AddLine = function(_, ...) tooltip:AddLine(...) end,
                    Show = function(_) tooltip:Show() end,
                }, leaderName, realm)
            end
        end)

        -- Hook CommunitiesFrame (Guild Roster) ScrollBox row tooltips
        local function HookCommunitiesGuildRows()
            local container = CommunitiesFrame and CommunitiesFrame.MemberList and CommunitiesFrame.MemberList.ScrollBox
            if not container then return end

            local function HookRow(frame)
                if frame.__ratedStatsHooked then return end
                frame.__ratedStatsHooked = true

                frame:HookScript("OnEnter", function(self)
                    local info = self.memberInfo
                    if not info or not info.name then return end

                    local name, realm = strsplit("-", info.name)
                    realm = realm or GetRealmName()
                    AddAchievementInfoToTooltip(GameTooltip, name, realm)
                end)
            end

        container:RegisterCallback("OnAcquiredFrame", function(_, frame)
            if type(frame) == "table" and frame.GetObjectType then
                HookRow(frame)
            end
        end, true)
    end

    C_Timer.After(2, HookCommunitiesGuildRows)
    elseif event == "UPDATE_MOUSEOVER_UNIT" then
        if UnitIsPlayer("mouseover") then
            local name, realm = UnitFullName("mouseover")
            if name then
                realm = realm or GetRealmName()
                AddAchievementInfoToTooltip(GameTooltip, name, realm)
                GameTooltip:Show()
            end
        end
    end
end)